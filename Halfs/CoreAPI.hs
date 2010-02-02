{-# LANGUAGE FlexibleContexts, ScopedTypeVariables #-}
module Halfs.CoreAPI where

import Control.Exception (assert)
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
import Halfs.HalfsState
import Halfs.Inode
import Halfs.Monad
import Halfs.Protection
import Halfs.SuperBlock
import Halfs.Types

import System.Device.BlockDevice

-- import Debug.Trace

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
         BlockDevice m -> HalfsM m SuperBlock
newfs dev = do
  when (superBlockSize > bdBlockSize dev) $
    fail "The device's block size is insufficiently large!"

  blockMap <- lift $ newBlockMap dev
  numFree  <- readRef (bmNumFree blockMap)
  rdirAddr <- lift (alloc1 blockMap) >>=
              maybe (fail "Unable to allocate block for rdir inode") return

  -- Build the root directory inode and persist it; note that we do not use
  -- Directory.makeDirectory here because this is a special case where we have
  -- no parent directory.

  let rdirInode = blockAddrToInodeRef rdirAddr
  dirInode <- lift $
              buildEmptyInodeEnc dev rdirInode nilInodeRef rootUser rootGroup
  assert (BS.length dirInode == fromIntegral (bdBlockSize dev)) $ do
  lift $ bdWriteBlock dev rdirAddr dirInode

  -- Persist the remaining data structures
  lift $ writeBlockMap dev blockMap
  lift $ writeSB       dev $ superBlock rdirInode (numFree - rdirBlks) rdirBlks
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
         BlockDevice m -> HalfsM m (HalfsState b r l m)
mount dev = do
  esb <- decode `fmap` lift (bdReadBlock dev 0)
  case esb of
    Left msg -> throwError $ HalfsMountFailed $ BadSuperBlock msg
    Right sb -> do
      if unmountClean sb
       then do
         sb' <- lift $ writeSB dev sb{ unmountClean = False }
         HalfsState dev
           `fmap` lift (readBlockMap dev)
           `ap`   newRef sb'
           `ap`   newLock
           `ap`   newRef M.empty
           `ap`   newLock 
       else throwError $ HalfsMountFailed DirtyUnmount

-- | Unmounts the given filesystem.  After this operation completes, the
-- superblock will have its unmountClean flag set to True.
unmount :: (HalfsCapable b t r l m) =>
           HalfsState b r l m -> HalfsM m ()
unmount fs@HalfsState{hsBlockDev = dev, hsSuperBlock = sbRef} = 
  withLock (hsLock fs)      $ do 
  withLock (hsDHMapLock fs) $ do   
  -- ^ Grab everything; we do not want to permit other filesystem actions to
  -- occur in other threads during or after teardown. Needs testing: TODO
  sb <- readRef sbRef
  if (unmountClean sb)
   then throwError HalfsUnmountFailed
   else do

     -- TODO:
     -- * Persist any dirty data structures (dirents, files w/ buffered IO, etc)

     -- Sync all directories; clean state is a no-op
     mapM_ (syncDirectory fs) =<< M.elems `fmap` readRef (hsDHMap fs)

     lift $ bdFlush dev
   
     -- Finalize the superblock
     let sb' = sb{ unmountClean = True }
     writeRef sbRef sb'
     lift $ writeSB dev sb'
     return ()
  
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
         HalfsState b r l m
      -> FileMode
      -> FilePath
      -> HalfsM m ()
mkdir fs fm fp = do
  parentIR <- absPathIR fs path Directory 
  u <- getUser
  g <- getGroup
  makeDirectory fs parentIR dirName u g fm
  return ()
  where
    (path, dirName) = splitFileName fp

rmdir :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FilePath -> HalfsM m ()
rmdir = undefined

openDir :: (HalfsCapable b t r l m) =>
           HalfsState b r l m -> FilePath -> HalfsM m (DirHandle r l)
openDir fs fp = absPathIR fs fp Directory >>= openDirectory fs

closeDir :: (HalfsCapable b t r l m) =>
            HalfsState b r l m -> DirHandle r l -> HalfsM m ()
closeDir fs dh = closeDirectory fs dh

readDir :: (HalfsCapable b t r l m) =>
           HalfsState b r l m
        -> DirHandle r l
        -> HalfsM m [(FilePath, FileStat t)]
readDir _fs dh =
  withLock (dhLock dh) $ do
    contents <- readRef $ dhContents dh
    return $ M.keys contents `zip`
               repeat (error "readDir Internal: FileStat aggregation NYI")

-- | Synchronize the given directory to disk.
syncDir :: (HalfsCapable b t r l m) =>
           HalfsState b r l m -> FilePath -> SyncType -> HalfsM m ()
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
-- * Yields HalfsObjectNotFound if the request file does not exist and create is
-- false
--
-- * Otherwise, provides a FileHandle to the requested file

-- TODO: modes and flags for open: append, r/w, ronly, truncate, etc., and
-- enforcement of the same
openFile :: (HalfsCapable b t r l m) =>
            HalfsState b r l m -- ^ The FS
         -> FilePath           -- ^ The absolute path of the file
         -> Bool               -- ^ Should we create the file if it is not
                               --   found?
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
          fir <- createFile
                   fs
                   parentDH
                   fname
                   usr
                   grp
                   defaultFilePerms -- FIXME
          fh <- openFilePrim fir
          -- TODO/FIXME: mark this FH as open in FD structures &
          -- r/w'able perms etc.?
          return fh
        else do
          throwError $ HalfsFileNotFound
    --
    foundFile fir = do
      if creat
       then do
         throwError $ HalfsObjectExists fp
       else do
         fh <- openFilePrim fir
         -- TODO/FIXME: mark this FH as open in FD structures, store
         -- r/w'able perms etc.?
         return fh
                    
read :: (HalfsCapable b t r l m) =>
        HalfsState b r l m  -- ^ the filesystem
     -> FileHandle          -- ^ the handle for the open file to read
     -> Word64              -- ^ the byte offset into the file
     -> Word64              -- ^ the number of bytes to read
     -> HalfsM m ByteString -- ^ the data read
read fs fh byteOff len = do
  -- TODO: check fh modes & perms (e.g., write only)
  readStream (hsBlockDev fs) (fhInode fh) byteOff (Just len)

write :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -- ^ the filesystem
      -> FileHandle         -- ^ the handle for the open file to write
      -> Word64             -- ^ the byte offset into the file
      -> ByteString         -- ^ the data to write
      -> HalfsM m ()
write fs fh byteOff bytes = 
  -- TODO: check fh modes & perms (e.g., read only, not owner, etc)
  writeStream (hsBlockDev fs) (hsBlockMap fs) (fhInode fh) byteOff False bytes
  -- TODO: process errors from writeStream?

flush :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FileHandle -> HalfsM m ()
flush = undefined

syncFile :: (HalfsCapable b t r l m) =>
            HalfsState b r l m -> FilePath -> SyncType ->HalfsM m ()
syncFile = undefined

closeFile :: (HalfsCapable b t r l m) =>
             HalfsState b r l m -- ^ the filesystem
          -> FileHandle         -- ^ the handle to the open file to close
          -> HalfsM m ()
closeFile _fs _fh = do
  -- TODO/FIXME: sync, mark fh closed in FD structures
  return ()

setFileSize :: (HalfsCapable b t r l m) =>
               HalfsState b r l m -> FilePath -> Word64 -> HalfsM m ()
setFileSize = undefined

setFileTimes :: (HalfsCapable b t r l m) =>
                HalfsState b r l m -> FilePath -> t -> t -> HalfsM m ()
setFileTimes = undefined

rename :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> FilePath -> FilePath -> HalfsM m ()
rename = undefined


--------------------------------------------------------------------------------
-- Access control

chmod :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FilePath -> FileMode -> HalfsM m ()
chmod = undefined

chown :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FilePath -> UserID -> GroupID -> HalfsM m ()
chown = undefined

-- | JS XXX/TODO: What's the intent of this function?
access :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> FilePath -> [AccessRight] -> HalfsM m ()
access = undefined


--------------------------------------------------------------------------------
-- Link manipulation

mklink :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> FilePath -> FilePath -> HalfsM m ()
mklink = undefined

rmlink :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> FilePath -> HalfsM m ()
rmlink = undefined

createSymLink :: (HalfsCapable b t r l m) =>
                 HalfsState b r l m -> FilePath -> FilePath -> HalfsM m ()
createSymLink = undefined

readSymLink :: (HalfsCapable b t r l m) =>
               HalfsState b r l m -> FilePath -> HalfsM m FilePath
readSymLink = undefined


--------------------------------------------------------------------------------
-- Filesystem stats

fstat :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FilePath -> HalfsM m (FileStat t)
fstat = undefined

fsstat :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> HalfsM m FileSystemStats
fsstat = undefined


--------------------------------------------------------------------------------
-- Utility functions

withDir :: HalfsCapable b t r l m =>
           HalfsState b r l m
        -> FilePath
        -> (DirHandle r l -> HalfsM m a)
        -> HalfsM m a
withDir fs fp act = do
  dh   <- openDir fs fp
  rslt <- act dh
  closeDir fs dh
  return rslt

-- | Find the InodeRef corresponding to the given path, and call the given
-- function with it.  On error, yields HalfsPathComponentNotFound or
-- HalfsAbsolutePathExpected.
absPathIR :: HalfsCapable b t r l m =>
             HalfsState b r l m
          -> FilePath
          -> FileType
          -> HalfsM m InodeRef
absPathIR fs fp ftype = do
  if isAbsolute fp
   then do
     rdirIR <- rootDir `fmap` readRef (hsSuperBlock fs)
     mir    <- find fs rdirIR ftype (drop 1 $ splitDirectories fp)
     case mir of
       Nothing -> throwError $ HalfsPathComponentNotFound fp
       Just ir -> return ir
   else
     throwError HalfsAbsolutePathExpected

writeSB :: (HalfsCapable b t r l m) =>
           BlockDevice m -> SuperBlock -> m SuperBlock
writeSB dev sb = do 
  assert (BS.length sbdata <= fromIntegral (bdBlockSize dev)) $ return ()
  bdWriteBlock dev 0 sbdata
  bdFlush dev
  return sb
  where
    sbdata = encode sb

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
