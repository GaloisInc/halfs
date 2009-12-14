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

newfs :: (HalfsCapable b t r l m) => BlockDevice m -> m SuperBlock
newfs dev = do
  when (superBlockSize > bdBlockSize dev) $
    fail "The device's block size is insufficiently large!"

  let rdirBlks = 1
  blockMap <- newBlockMap dev
  numFree  <- sreadRef (bmNumFree blockMap)
  res      <- allocBlocks blockMap rdirBlks
  case res of
    Just (Contig rdirExt) -> do
      let rdirAddr  = extBase rdirExt
          rdirInode = blockAddrToInodeRef rdirAddr
      makeDirectory dev rdirAddr rdirInode rootUser rootGroup
      writeBlockMap dev blockMap
      writeSB       dev $ superBlock rdirInode (numFree - rdirBlks) rdirBlks
    _ -> 
      fail "Could not generate root directory!"
 where
   superBlock rdirIR nf nu = SuperBlock {
     version        = 1
   , blockSize      = bdBlockSize dev
   , blockCount     = bdNumBlocks dev
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
         BlockDevice m
      -> HalfsM m (Halfs b r m l)
mount dev = 
  decode `fmap` bdReadBlock dev 0 >>=
  either
    (return . Left . HalfsMountFailed . BadSuperBlock)
    (\sb -> do
       if unmountClean sb then do
         sb' <- writeSB dev sb{ unmountClean = False }
         Right `fmap`
           (HalfsState dev
            `fmap` readBlockMap dev       
            `ap`   newRef sb'
            `ap`   newLock
           )
        else
          return $ Left $ HalfsMountFailed DirtyUnmount
    )

-- | Unmounts the given filesystem.  After this operation completes, the
-- superblock will have its unmountClean flag set to True.
unmount :: (HalfsCapable b t r l m) =>
           Halfs b r m l ->
           HalfsM m ()
unmount fs@HalfsState{hsBlockDev = dev, hsSuperBlock = sbRef} = do
  locked fs $ do
    sb <- sreadRef sbRef
    if (unmountClean sb) then
      return $ Left HalfsUnmountFailed
     else do
       -- TODO:
       -- * Persist any dirty data structures (dirents, files w/ buffered IO, etc)
       bdFlush dev
     
       -- Finalize the superblock
       let sb' = sb{ unmountClean = True }
       swriteRef sbRef sb'
       writeSB dev sb'
       return $ Right $ ()
  
fsck :: Int
fsck = undefined

--------------------------------------------------------------------------------
-- Directory manipulation

mkdir :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath -> FileMode ->
         HalfsM m ()
mkdir = undefined

rmdir :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath ->
         HalfsM m ()
rmdir = undefined


openDir :: (HalfsCapable b t r l m) =>
           Halfs b r m l -> FilePath ->
           HalfsM m (DirHandle r)
openDir = undefined

closeDir :: (HalfsCapable b t r l m) =>
            Halfs b r m l -> (DirHandle r) ->
            HalfsM m ()
closeDir = undefined

readDir :: (HalfsCapable b t r l m) =>
           Halfs b r m l -> (DirHandle r) ->
           HalfsM m [(FilePath, FileStat t)]
readDir = undefined

-- | Synchronize the given directory to disk.
syncDir :: (HalfsCapable b t r l m) =>
           Halfs b r m l -> FilePath -> SyncType ->
           HalfsM m ()
syncDir = undefined

--------------------------------------------------------------------------------
-- File manipulation

openFile :: (HalfsCapable b t r l m) =>
            Halfs b r m l -> FilePath ->
            HalfsM m FileHandle
openFile = undefined

read :: (HalfsCapable b t r l m) =>
        Halfs b r m l -> FileHandle -> Word64 -> Word64 ->
        HalfsM m ByteString
read = undefined

write :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FileHandle -> Word64 -> Word64 -> ByteString ->
         HalfsM m ()
write = undefined

flush :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath ->
         HalfsM m ()
flush = undefined

syncFile :: (HalfsCapable b t r l m) =>
            Halfs b r m l -> FilePath -> SyncType ->
            HalfsM m ()
syncFile = undefined

closeFile :: (HalfsCapable b t r l m) =>
             Halfs b r m l -> FilePath ->
             HalfsM m ()
closeFile = undefined

setFileSize :: (HalfsCapable b t r l m) =>
               Halfs b r m l -> FilePath -> Word64 ->
               HalfsM m ()
setFileSize = undefined

setFileTimes :: (HalfsCapable b t r l m) =>
                Halfs b r m l -> FilePath -> t -> t ->
                HalfsM m ()
setFileTimes = undefined

rename :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> FilePath -> FilePath ->
          HalfsM m ()
rename = undefined

--------------------------------------------------------------------------------
-- Access control

chmod :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath -> FileMode ->
         HalfsM m ()
chmod = undefined

chown :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath -> UserID -> GroupID ->
         HalfsM m ()
chown = undefined

-- | JS XXX/TODO: What's the intent of this function?
access :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> FilePath -> [AccessRight] ->
          HalfsM m ()
access = undefined

--------------------------------------------------------------------------------
-- Link manipulation

mklink :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> FilePath -> FilePath ->
          HalfsM m ()
mklink = undefined

rmlink :: (HalfsCapable b t r l m) =>
          Halfs b r m l -> FilePath ->
          HalfsM m ()
rmlink = undefined

createSymLink :: (HalfsCapable b t r l m) =>
                 Halfs b r m l -> FilePath -> FilePath ->
                 HalfsM m ()
createSymLink = undefined

readSymLink :: (HalfsCapable b t r l m) =>
               Halfs b r m l -> FilePath ->
               HalfsM m FilePath
readSymLink = undefined

--------------------------------------------------------------------------------
-- Filesystem stats

fstat :: (HalfsCapable b t r l m) =>
         Halfs b r m l -> FilePath ->
         HalfsM m (FileStat t)
fstat = undefined

fsstat :: (HalfsCapable b t r l m) =>
          Halfs b r m l ->
          HalfsM m FileSystemStats
fsstat = undefined

--------------------------------------------------------------------------------
-- Utility functions

writeSB :: HalfsCapable b t r l m => BlockDevice m -> SuperBlock -> m SuperBlock
writeSB dev sb =
  let sbdata = encode sb in 
  assert (BS.length sbdata <= fromIntegral (bdBlockSize dev)) $ do
    bdWriteBlock dev 0 sbdata
    bdFlush dev
    return sb

locked :: HalfsCapable b t r l m => Halfs b r m l -> HalfsM m a -> HalfsM m a
locked fs act = do
  lock $ hsLock fs
  res <- act
  release $ hsLock fs
  return res

-- Strict readRef
sreadRef :: Reffable r m => r a -> m a
sreadRef = ($!) readRef

-- Strict writeRef
swriteRef :: Reffable r m => r a -> a -> m ()
swriteRef = ($!) writeRef
            
           
