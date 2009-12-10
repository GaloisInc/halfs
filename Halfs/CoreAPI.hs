{-# LANGUAGE FlexibleContexts, ScopedTypeVariables #-}
module Halfs.CoreAPI where

import Control.Monad.Reader
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Data.Serialize
import Data.Word
import System.FilePath

import Halfs.BlockMap
import Halfs.Directory
import Halfs.File
import Halfs.Inode
import Halfs.Monad
import Halfs.Protection
import Halfs.SuperBlock
import System.Device.BlockDevice

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

-- |Create a new file system on the given device. The new file system will use
-- the block size reported by bdBlockSize on the device; if you want to increase
-- the file system's block size, you should use 'newRescaledBlockDevice' from
-- System.Device.BlockDevice.
--
-- Block size has no bearing on the maximum size of a file or directory, but
-- will impact the size of certain internal data structures that keep track of
-- free blocks. Larger block sizes will decrease the cost of these structures
-- per underlying disk megabyte, but may also lead to more wasted space for
-- small files and directories.
newfs :: (HalfsCapable b t r l m) => BlockDevice m -> m ()
newfs dev = do
  when (superBlockSize > bdBlockSize dev) $
    fail "The device's block size is insufficiently large!"
  blockMap <- newBlockMap dev
  res      <- allocBlocks blockMap 1
  case res of
    Just (Contig rdirExt) -> do
      let rdirAddr              = extBase rdirExt
          rdirInode :: InodeRef = blockAddrToInodeRef rdirAddr
      writeBlockMap dev blockMap
      makeDirectory dev rdirAddr rdirInode rootUser rootGroup
      bdWriteBlock  dev 0 (superBlockBstr rdirInode)
    _ -> 
      fail "Could not generate root directory!"
 where
  superBlockBstr r = encode $ superBlock r
  superBlock rdir  = SuperBlock {
    version        = 1
  , blockSize      = bdBlockSize dev
  , blockCount     = bdNumBlocks dev
  , unmountClean   = True
  , freeBlocks     = -- Was: bdNumBlocks dev - 1 -- FIXME
      error "freeBlocks calculation not yet implemented"
  , usedBlocks     = -- Was: 1                   -- FIXME
      error "usedBlocks calculation not yet implemented"
  , fileCount      = 0
  , rootDir        = rdir
  , blockMapStart  = blockAddrToInodeRef 1
  }

mount :: (HalfsCapable b t r l m) => BlockDevice m -> Halfs b r m l
mount = undefined

unmount :: (HalfsCapable b t r l m) =>
           Halfs b r m l ->
           HalfsM m ()
unmount = undefined

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
