{-# LANGUAGE FlexibleContexts, ExistentialQuantification #-}
module Halfs.CoreAPI where

import Control.Monad.Reader
import Data.Array.MArray
import Data.Binary
import Data.ByteString.Lazy(ByteString)
import qualified Data.ByteString.Lazy as BS
import Data.Word
import System.FilePath

import Halfs.BlockMap
import Halfs.Classes(Timed(..))
import Halfs.Directory
import Halfs.Errors
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
-- newfs :: (HalfsCapable a t r l m) => BlockDevice m -> m ()
newfs dev = do
  when (fromIntegral (BS.length superBlockBstr) > bdBlockSize dev) $
    fail "The device's block size is insufficiently large!"
  forM_ [1..bitmapNumBlocks] $ \ i -> do
    bdWriteBlock dev i $ BS.take blockSizeI $ BS.repeat 0
  blockMap <- readBlockMap dev
  markBlocksUsed blockMap 0 rootDirAddr
  writeBlockMap dev blockMap
  makeDirectory dev rootDirAddr rootDirInode rootUser rootGroup
  bdWriteBlock  dev 0 superBlockBstr
 where
  blockSize       = bdBlockSize dev
  blockSizeI      = fromIntegral blockSize
  bitmapNumBlocks = blockMapSizeBlocks $ bdNumBlocks dev
  rootDirAddr     = bitmapNumBlocks + 1
  rootDirInode    = blockAddrToInodeRef rootDirAddr
  --
  superBlockBstr  = encode superBlock
  superBlock      = SuperBlock {
    version       = 1
  , blockSize     = blockSize
  , blockCount    = bdNumBlocks dev
  , unmountClean  = True
  , freeBlocks    = bdNumBlocks dev - 1
  , usedBlocks    = 1
  , fileCount     = 0
  , rootDir       = fromIntegral $! bitmapNumBlocks + 1
  , blockList     = fromIntegral 1
  }

mount :: (HalfsCapable a t r l m) => BlockDevice m -> Halfs a r m l
mount = undefined

fsck :: Int
fsck = undefined

fstat :: (HalfsCapable a t r l m) =>
         Halfs a r m l -> FilePath ->
         HalfsM m (FileStat t)
fstat = undefined

readSymLink :: (HalfsCapable a t r l m) =>
               Halfs a r m l -> FilePath ->
               HalfsM m FilePath
readSymLink = undefined

mkdir :: (HalfsCapable a t r l m) =>
         Halfs a r m l -> FilePath -> FileMode ->
         HalfsM m ()
mkdir = undefined

rmlink :: (HalfsCapable a t r l m) =>
          Halfs a r m l -> FilePath ->
          HalfsM m ()
rmlink = undefined

rmdir :: (HalfsCapable a t r l m) =>
         Halfs a r m l -> FilePath ->
         HalfsM m ()
rmdir = undefined

createSymLink :: (HalfsCapable a t r l m) =>
                 Halfs a r m l -> FilePath -> FilePath ->
                 HalfsM m ()
createSymLink = undefined

rename :: (HalfsCapable a t r l m) =>
          Halfs a r m l -> FilePath -> FilePath ->
          HalfsM m ()
rename = undefined

mklink :: (HalfsCapable a t r l m) =>
          Halfs a r m l -> FilePath -> FilePath ->
          HalfsM m ()
mklink = undefined

setFileMode :: (HalfsCapable a t r l m) =>
               Halfs a r m l -> FilePath -> FileMode ->
               HalfsM m ()
setFileMode = undefined

chown :: (HalfsCapable a t r l m) =>
         Halfs a r m l -> FilePath -> UserID -> GroupID ->
         HalfsM m ()
chown = undefined

setFileSize :: (HalfsCapable a t r l m) =>
               Halfs a r m l -> FilePath -> Word64 ->
               HalfsM m ()
setFileSize = undefined

setFileTimes :: (HalfsCapable a t r l m) =>
                Halfs a r m l -> FilePath -> t -> t ->
                HalfsM m ()
setFileTimes = undefined

openFile :: (HalfsCapable a t r l m) =>
            Halfs a r m l -> FilePath ->
            HalfsM m FileHandle
openFile = undefined

read :: (HalfsCapable a t r l m) =>
        Halfs a r m l -> FileHandle -> Word64 -> Word64 ->
        HalfsM m ByteString
read = undefined

write :: (HalfsCapable a t r l m) =>
         Halfs a r m l -> FileHandle -> Word64 -> Word64 -> ByteString ->
         HalfsM m ()
write = undefined

fsstat :: (HalfsCapable a t r l m) =>
          Halfs a r m l ->
          HalfsM m FileSystemStats
fsstat = undefined

flush :: (HalfsCapable a t r l m) =>
         Halfs a r m l -> FilePath ->
         HalfsM m ()
flush = undefined

closeFile :: (HalfsCapable a t r l m) =>
             Halfs a r m l -> FilePath ->
             HalfsM m ()
closeFile = undefined

syncFile :: (HalfsCapable a t r l m) =>
            Halfs a r m l -> FilePath -> SyncType ->
            HalfsM m ()
syncFile = undefined

openDir :: (HalfsCapable a t r l m) =>
           Halfs a r m l -> FilePath ->
           HalfsM m (DirHandle r)
openDir = undefined

readDir :: (HalfsCapable a t r l m) =>
           Halfs a r m l -> (DirHandle r) ->
           HalfsM m [(FilePath, FileStat t)]
readDir = undefined

closeDir :: (HalfsCapable a t r l m) =>
            Halfs a r m l -> (DirHandle r) ->
            HalfsM m ()
closeDir = undefined

-- |Synchronize the given directory to disk.
syncDir :: (HalfsCapable a t r l m) =>
           Halfs a r m l -> FilePath -> SyncType ->
           HalfsM m ()
syncDir = undefined

access :: (MArray a bool m, Timed t m) =>
          Halfs a r m l -> FilePath -> [AccessRight] ->
          HalfsM m ()
access = undefined

