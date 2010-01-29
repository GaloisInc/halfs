{-# LANGUAGE FlexibleContexts, ScopedTypeVariables #-}
module Halfs.CoreAPI where

import Control.Exception (assert)
import Control.Monad.Reader
import Data.ByteString   (ByteString)
import qualified Data.ByteString as BS
import qualified Data.Map        as M
import Data.Serialize
import Data.Word
import System.FilePath

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Directory
import Halfs.Errors
import Halfs.File
import Halfs.Inode
import Halfs.Monad
import Halfs.Protection
import Halfs.SuperBlock
import Halfs.Utils

import System.Device.BlockDevice

import Debug.Trace

data SyncType = Data | Everything

data FileSystemStats = FSS {
    fssBlockSize     :: Integer
  , fssBlockCount    :: Integer
  , fssBlocksFree    :: Integer
  , fssBlocksAvail   :: Integer
  , fssFileCount     :: Integer
  , fssMaxNameLength :: Integer
  }


--------------------------------------------------------------------------------
-- Filesystem init, teardown, and check functions

-- | Create a new file system on the given device. The new file system
-- will use the block size reported by bdBlockSize on the device; if you
-- want to increase the file system's block size, you should use
-- 'newRescaledBlockDevice' from System.Device.BlockDevice.
--
-- Block size has no bearing on the maximum size of a file or directory, but
-- will impact the size of certain internal data structures that keep track of
-- free blocks. Larger block sizes will decrease the cost of these structures
-- per underlying disk megabyte, but may also lead to more wasted space for
-- small files and directories.

newfs :: (HalfsCapable b t r l m) =>
         BlockDevice m -> m SuperBlock
newfs dev = do
  when (superBlockSize > bdBlockSize dev) $
    fail "The device's block size is insufficiently large!"

  blockMap <- newBlockMap dev
  numFree  <- readRef (bmNumFree blockMap)
  rdirAddr <- alloc1 blockMap >>=
              maybe (fail "Unable to allocate block for rdir inode") return

  -- Build the root directory inode and persist it; note that we do not use
  -- Directory.makeDirectory here because this is a special case where we have
  -- no parent directory.

  let rdirInode = blockAddrToInodeRef rdirAddr
  dirInode <- buildEmptyInodeEnc dev rdirInode nilInodeRef rootUser rootGroup
  assert (BS.length dirInode == fromIntegral (bdBlockSize dev)) $ do
  bdWriteBlock dev rdirAddr dirInode

  -- Persist the remaining data structures
  writeBlockMap dev blockMap
  writeSB       dev $ superBlock rdirInode (numFree - rdirBlks) rdirBlks
 where
   rdirBlks                = 1
   superBlock rdirIR nf nu = SuperBlock {
     version        = 1
   , devBlockSize   = bdBlockSize dev
   , devBlockCount  = bdNumBlocks dev
   , unmountClean   = True
   , freeBlocks     = nf 
   , usedBlocks     = nu
   , fileCount      = 0
   , rootDir        = rdirIR
   , blockMapStart  = blockAddrToInodeRef 1
   }

-- | Mounts a filesystem from a given block device.  After this operation
-- completes, the superblock will have its unmountClean flag set to False.
mount :: (HalfsCapable b t r l m) =>
         BlockDevice m -> HalfsM m (Halfs b r l m)
mount dev = do
  esb <- decode `fmap` bdReadBlock dev 0
  case esb of
    Left msg -> return $ Left $ HalfsMountFailed $ BadSuperBlock msg
    Right sb -> do
      if unmountClean sb
       then do
         sb' <- writeSB dev sb{ unmountClean = False }
         Right `fmap`
           (HalfsState dev
            `fmap` readBlockMap dev       
            `ap`   newRef sb'
            `ap`   newLock
           )
       else do 
         return $ Left $ HalfsMountFailed DirtyUnmount

-- | Unmounts the given filesystem.  After this operation completes, the
-- superblock will have its unmountClean flag set to True.
unmount :: (HalfsCapable b t r l m) =>
           Halfs b r l m -> HalfsM m ()
unmount fs@HalfsState{hsBlockDev = dev, hsSuperBlock = sbRef} = 
  locked fs $ do
  sb <- readRef sbRef
  if (unmountClean sb)
   then do
     return $ Left HalfsUnmountFailed
   else do
     -- TODO:
     -- * Persist any dirty data structures (dirents, files w/ buffered IO, etc)
     bdFlush dev
   
     -- Finalize the superblock
     let sb' = sb{ unmountClean = True }
     writeRef sbRef sb'
     writeSB dev sb'
     return $ Right $ ()
  
fsck :: Int
fsck = undefined


--------------------------------------------------------------------------------
-- Directory manipulation

-- | Makes a directory given an absolute path.
--
-- * Yields HalfsPathComponentNotFound if any path component on the way down to
--   the directory to create does not exist.
--
-- * Yields HalfsAbsolutePathExpected if an absolute path is not provided.
--
mkdir :: (HalfsCapable b t r l m) =>
         Halfs b r l m -> FilePath -> FileMode -> HalfsM m ()
mkdir fs fp fm = do
  withAbsPathIR fs path Directory $ \parentIR -> do
  u <- getUser
  g <- getGroup
  either Left (const $ Right ()) `fmap`
    makeDirectory fs parentIR dirName u g fm
  where
    (path, dirName) = splitFileName fp

rmdir :: (HalfsCapable b t r l m) =>
         Halfs b r l m -> FilePath -> HalfsM m ()
rmdir = undefined

openDir :: (HalfsCapable b t r l m) =>
           Halfs b r l m -> FilePath -> HalfsM m (DirHandle r)
openDir fs fp =
  withAbsPathIR fs fp Directory $ \dirIR -> do
  Right `fmap` openDirectory (hsBlockDev fs) dirIR

closeDir :: (HalfsCapable b t r l m) =>
            Halfs b r l m -> DirHandle r -> HalfsM m ()
closeDir fs dh = Right `fmap` closeDirectory fs dh

readDir :: (HalfsCapable b t r l m) =>
           Halfs b r l m -> DirHandle r -> HalfsM m [(FilePath, FileStat t)]
readDir = undefined

-- | Synchronize the given directory to disk.
syncDir :: (HalfsCapable b t r l m) =>
           Halfs b r l m -> FilePath -> SyncType -> HalfsM m ()
syncDir = undefined

--------------------------------------------------------------------------------
-- File manipulation

-- | Opens a file given an absolute path.
--
-- * Yields HalfsPathComponentNotFound if any path component on the way down to
-- the file does not exist.
--
-- * Yields HalfsAbsolutePathExpected if an absolute path is not provided
--
-- * Yields HalfsFileNotFound if the request file does not exist and create is
-- false
--
-- * Otherwise, provides a FileHandle to the requested file

-- TODO: modes and flags for open: append, r/w, ronly, truncate, etc., and
-- enforcement of the same
openFile :: (HalfsCapable b t r l m) =>
            Halfs b r l m -- ^ The FS
         -> FilePath      -- ^ The absolute path of the file
         -> Bool          -- ^ Should we create the file if it is not found?
         -> HalfsM m FileHandle 
openFile fs fp creat = do
  withDir fs path $ \pdh -> do 
    findInDir pdh fname RegularFile >>= maybe (noFile pdh) foundFile
  where
    (path, fname) = splitFileName fp
    -- 
    noFile parentDH =
      if creat
        then do
          usr <- getUser
          grp <- getGroup
          whenOK ( createFile
                     fs
                     parentDH
                     fname
                     usr
                     grp
                     defaultFilePerms -- FIXME
                 ) $
            \fir -> do
              fh <- openFilePrim fir
              -- TODO/FIXME: mark this FH as open & r/w'able perms etc.?
              return $ Right fh
        else do
          return $ Left $ HalfsFileNotFound
    --
    foundFile fir = do
      if creat
       then do
         return $ Left $ HalfsFileExists fp
       else do
         fh <- openFilePrim fir
         -- TODO/FIXME: mark this FH as open, store r/w'able perms etc.?
         return $ Right fh
                    
read :: (HalfsCapable b t r l m) =>
        Halfs b r l m       -- ^ the filesystem
     -> FileHandle          -- ^ the handle for the open file to read
     -> Word64              -- ^ the byte offset into the file
     -> Word64              -- ^ the number of bytes to read
     -> HalfsM m ByteString -- ^ the data read
read = undefined

write :: (HalfsCapable b t r l m) =>
         Halfs b r l m -- ^ the filesystem
      -> FileHandle    -- ^ the handle for the open file to write
      -> Word64        -- ^ the byte offset into the file
      -> ByteString    -- ^ the data to write
      -> HalfsM m ()
write fs fh byteOff bytes = 
  -- TODO: check fh modes (e.g., read only)
  -- TODO: check perms
  writeStream (hsBlockDev fs) (hsBlockMap fs) (fhInode fh) byteOff False bytes

flush :: (HalfsCapable b t r l m) =>
         Halfs b r l m -> FileHandle -> HalfsM m ()
flush = undefined

syncFile :: (HalfsCapable b t r l m) =>
            Halfs b r l m -> FilePath -> SyncType ->HalfsM m ()
syncFile = undefined

closeFile :: (HalfsCapable b t r l m) =>
             Halfs b r l m -> FileHandle -> HalfsM m ()
closeFile fs fh = do
  -- TODO/FIXME: sync, mark fh closed
  return $ Right ()

setFileSize :: (HalfsCapable b t r l m) =>
               Halfs b r l m -> FilePath -> Word64 -> HalfsM m ()
setFileSize = undefined

setFileTimes :: (HalfsCapable b t r l m) =>
                Halfs b r l m -> FilePath -> t -> t -> HalfsM m ()
setFileTimes = undefined

rename :: (HalfsCapable b t r l m) =>
          Halfs b r l m -> FilePath -> FilePath -> HalfsM m ()
rename = undefined


--------------------------------------------------------------------------------
-- Access control

chmod :: (HalfsCapable b t r l m) =>
         Halfs b r l m -> FilePath -> FileMode -> HalfsM m ()
chmod _ _ _ = trace ("WARNING: chmod NYI") $ return $ Right ()

chown :: (HalfsCapable b t r l m) =>
         Halfs b r l m -> FilePath -> UserID -> GroupID -> HalfsM m ()
chown = undefined

-- | JS XXX/TODO: What's the intent of this function?
access :: (HalfsCapable b t r l m) =>
          Halfs b r l m -> FilePath -> [AccessRight] -> HalfsM m ()
access = undefined


--------------------------------------------------------------------------------
-- Link manipulation

mklink :: (HalfsCapable b t r l m) =>
          Halfs b r l m -> FilePath -> FilePath -> HalfsM m ()
mklink = undefined

rmlink :: (HalfsCapable b t r l m) =>
          Halfs b r l m -> FilePath -> HalfsM m ()
rmlink = undefined

createSymLink :: (HalfsCapable b t r l m) =>
                 Halfs b r l m -> FilePath -> FilePath -> HalfsM m ()
createSymLink = undefined

readSymLink :: (HalfsCapable b t r l m) =>
               Halfs b r l m -> FilePath -> HalfsM m FilePath
readSymLink = undefined


--------------------------------------------------------------------------------
-- Filesystem stats

fstat :: (HalfsCapable b t r l m) =>
         Halfs b r l m -> FilePath -> HalfsM m (FileStat t)
fstat = undefined

fsstat :: (HalfsCapable b t r l m) =>
          Halfs b r l m -> HalfsM m FileSystemStats
fsstat = undefined


--------------------------------------------------------------------------------
-- Utility functions

withDir :: HalfsCapable b t r l m =>
           Halfs b r l m
        -> FilePath
        -> (DirHandle r -> HalfsM m a)
        -> HalfsM m a
withDir fs fp act = do
  whenOK (openDir fs fp) $ \dh -> do
    rslt <- act dh
    closeDir fs dh
    return rslt

-- | Find the InodeRef corresponding to the given path, and call the given
-- function with it.  On error, yields HalfsPathComponentNotFound or
-- HalfsAbsolutePathExpected.
withAbsPathIR :: HalfsCapable b t r l m =>
                 Halfs b r l m
              -> FilePath
              -> FileType
              -> (InodeRef -> HalfsM m a)
              -> HalfsM m a
withAbsPathIR fs fp ftype f = do
  trace ("withAbsPathIR: fp = " ++ show fp ++ ", ftype = " ++ show ftype) $ do
  if isAbsolute fp
   then do
     rdirIR <- rootDir `fmap` readRef (hsSuperBlock fs)
     mir    <- find (hsBlockDev fs) rdirIR ftype
                 (drop 1 $ splitDirectories fp)
     case mir of
       Nothing -> return $ Left $ HalfsPathComponentNotFound fp
       Just ir -> f ir
   else do
     return $ Left $ HalfsAbsolutePathExpected

writeSB :: (HalfsCapable b t r l m) =>
           BlockDevice m -> SuperBlock -> m SuperBlock
writeSB dev sb = do 
  assert (BS.length sbdata <= fromIntegral (bdBlockSize dev)) $ return ()
  bdWriteBlock dev 0 sbdata
  bdFlush dev
  return sb
  where
    sbdata = encode sb

locked :: (HalfsCapable b t r l m) =>
          Halfs b r l m -> HalfsM m a -> HalfsM m a
locked fs act = do
  lock $ hsLock fs
  res <- act
  release $ hsLock fs
  return res

-- TODO: Placeholder
getUser :: Monad m => m UserID
getUser = return rootUser

-- TODO: Placeholder
getGroup :: Monad m => m GroupID
getGroup = return rootGroup

-- TODO: Placeholder
defaultDirPerms :: FileMode
defaultDirPerms = FileMode [Read,Write,Execute] [Read, Execute] [Read, Execute]

-- TODO: Placeholder
defaultFilePerms :: FileMode
defaultFilePerms = FileMode [Read,Write] [Read] [Read]


--------------------------------------------------------------------------------
-- Debugging helpers

dumpfs :: HalfsCapable b t r l m =>
          Halfs b r l m -> m String
dumpfs fs = do
  dump <- dumpfs' 2 "/\n" =<< rootDir `fmap` readRef (hsSuperBlock fs)
  return $ "=== fs dump begin ===\n" ++ dump  ++ "=== fs dump end ===\n"
  where
    dumpfs' i pfx inr = do 
      let pre = replicate i ' '            
      dh       <- openDirectory (hsBlockDev fs) inr
      contents <- readRef $ dhContents dh
      foldM (\dumpAcc (path, dirEnt) -> do
               sub <- if deType dirEnt == Directory
                      then dumpfs' (i+2) "" (deInode dirEnt)
                      else return ""
               return $ dumpAcc
                     ++ pre
                     ++ path
                     ++ case deType dirEnt of
                          RegularFile -> " (file)\n"
                          Directory   -> " (directory)\n" ++ sub
                          Symlink     -> " (symlink)\n"
            )
            pfx (M.toList contents)
