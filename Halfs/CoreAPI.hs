{-# LANGUAGE FlexibleContexts, ScopedTypeVariables #-}
module Halfs.CoreAPI where

import Control.Exception (assert)
import Control.Monad.Reader
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
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
  rdirAddr <- maybe (fail "Unable to allocate block for rdir inode") return
              =<< alloc1 blockMap

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
         BlockDevice m -> HalfsM m (Halfs b r m l)
mount dev = 
  decode `fmap` bdReadBlock dev 0 >>=
  either
    (return . Left . HalfsMountFailed . BadSuperBlock)
    (\sb -> do
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
    )

-- | Unmounts the given filesystem.  After this operation completes, the
-- superblock will have its unmountClean flag set to True.
unmount :: (HalfsCapable b t r l m) =>
           Halfs b r m l -> HalfsM m ()
unmount fs@HalfsState{hsBlockDev = dev, hsSuperBlock = sbRef} = do
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
         Halfs b r m l -> FilePath -> FileMode -> HalfsM m ()
mkdir fs fp fm = do
  usr       <- getUser
  grp       <- getGroup
  alloc1 (hsBlockMap fs) >>=
    maybe (return $ Left HalfsAllocFailed)
          (\dirAddr -> do
             mdirAddr  <- alloc1 $ hsBlockMap fs
             withAbsPathIR fs path Directory $ \parentIR -> do
               trace ("mkdir: parentIR = " ++ show parentIR) $ do               
               makeDirectory fs dirAddr parentIR dirName usr grp defaultDirPerms
               >>= either (return . Left) (const $ chmod fs fp fm)
          )
  where
    (path, dirName) = splitFileName fp

rmdir :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath -> HalfsM m ()
rmdir = undefined

openDir :: (HalfsCapable b t r l m) =>
           Halfs b r m l -> FilePath -> HalfsM m (DirHandle r)
openDir fs fp =
  withAbsPathIR fs fp Directory $ \dirIR ->
    Right `fmap` openDirectory (hsBlockDev fs) dirIR

closeDir :: (HalfsCapable b t r l m) =>
            Halfs b r m l -> DirHandle r -> HalfsM m ()
closeDir = undefined

readDir :: (HalfsCapable b t r l m) =>
           Halfs b r m l -> DirHandle r -> HalfsM m [(FilePath, FileStat t)]
readDir = undefined

-- | Synchronize the given directory to disk.
syncDir :: (HalfsCapable b t r l m) =>
           Halfs b r m l -> FilePath -> SyncType -> HalfsM m ()
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
            Halfs b r m l -- ^ The FS
         -> FilePath      -- ^ The absolute path of the file
         -> Bool          -- ^ Should we create the file if it is not found?
         -> HalfsM m FileHandle 
openFile fs fp creat = do
  whenOK (openDir fs path) $ \dh -> 
    findInDir dh fname RegularFile >>= maybe noFile foundFile
  where
    whenOK act f  = act >>= either (return . Left) f
    (path, fname) = splitFileName fp
    -- 
    noFile =
      if creat
        then do
          -- TODO: allocate new inode for file being created, etc. Should
          -- probably delegate to Directory.createFile or somesuch; finish
          -- Directory.openDirectory before worrying about this.
          fail "TODO: openFile for new files not yet implemented"
        else do
          return $ Left $ HalfsFileNotFound
    --
    foundFile _fileIR = do
      fail "TODO: openFile for existing files not yet implemented"
                    
read :: (HalfsCapable b t r l m) =>
        Halfs b r m l -> FileHandle -> Word64 -> Word64 -> HalfsM m ByteString
read = undefined

write :: (HalfsCapable b t r l m) =>
         Halfs b r m l
      -> FileHandle
      -> Word64
      -> Word64
      -> ByteString
      -> HalfsM m ()
write = undefined

flush :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FileHandle -> HalfsM m ()
flush = undefined

syncFile :: (HalfsCapable b t r l m) =>
            Halfs b r m l -> FilePath -> SyncType ->HalfsM m ()
syncFile = undefined

closeFile :: (HalfsCapable b t r l m) =>
             Halfs b r m l -> FileHandle -> HalfsM m ()
closeFile = undefined

setFileSize :: (HalfsCapable b t r l m) =>
               Halfs b r m l -> FilePath -> Word64 -> HalfsM m ()
setFileSize = undefined

setFileTimes :: (HalfsCapable b t r l m) =>
                Halfs b r m l -> FilePath -> t -> t -> HalfsM m ()
setFileTimes = undefined

rename :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> FilePath -> FilePath -> HalfsM m ()
rename = undefined


--------------------------------------------------------------------------------
-- Access control

chmod :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath -> FileMode -> HalfsM m ()
chmod _ _ _ = trace ("WARNING: chmod NYI") $ return $ Right ()

chown :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath -> UserID -> GroupID -> HalfsM m ()
chown = undefined

-- | JS XXX/TODO: What's the intent of this function?
access :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> FilePath -> [AccessRight] -> HalfsM m ()
access = undefined


--------------------------------------------------------------------------------
-- Link manipulation

mklink :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> FilePath -> FilePath -> HalfsM m ()
mklink = undefined

rmlink :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> FilePath -> HalfsM m ()
rmlink = undefined

createSymLink :: (HalfsCapable b t r l m) =>
                 Halfs b r m l -> FilePath -> FilePath -> HalfsM m ()
createSymLink = undefined

readSymLink :: (HalfsCapable b t r l m) =>
               Halfs b r m l -> FilePath -> HalfsM m FilePath
readSymLink = undefined


--------------------------------------------------------------------------------
-- Filesystem stats

fstat :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath -> HalfsM m (FileStat t)
fstat = undefined

fsstat :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> HalfsM m FileSystemStats
fsstat = undefined


--------------------------------------------------------------------------------
-- Utility functions

-- | Find the InodeRef corresponding to the given path, and call the given
-- function with it.  On error, yields HalfsPathComponentNotFound or
-- HalfsAbsolutePathExpected.
withAbsPathIR :: HalfsCapable b t r l m =>
                 Halfs b r m l
              -> FilePath
              -> FileType
              -> (InodeRef -> HalfsM m a)
              -> HalfsM m a
withAbsPathIR fs fp ftype f = do
  if isAbsolute fp
   then do
     rdirIR <- rootDir `fmap` readRef (hsSuperBlock fs)
     find (hsBlockDev fs) rdirIR ftype (drop 1 $ splitDirectories fp) >>= 
       maybe (return $ Left $ HalfsPathComponentNotFound fp) f
   else do
     return $ Left $ HalfsAbsolutePathExpected

writeSB :: (HalfsCapable b t r l m) =>
           BlockDevice m -> SuperBlock -> m SuperBlock
writeSB dev sb =
  let sbdata = encode sb in 
  assert (BS.length sbdata <= fromIntegral (bdBlockSize dev)) $ do
    bdWriteBlock dev 0 sbdata
    bdFlush dev
    return sb

locked :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> HalfsM m a -> HalfsM m a
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
