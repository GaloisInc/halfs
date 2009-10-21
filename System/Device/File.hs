module System.Device.File(
         newFileBlockDevice
       )
 where

import qualified Data.ByteString as BS
import Data.Word
import System.IO

import System.Device.BlockDevice

-- |Use an existing file (specified by the first argument) as a block device.
-- The second argument is the block size for the pseudo-disk.
newFileBlockDevice :: FilePath -> Word64 -> IO (Maybe (BlockDevice IO))
newFileBlockDevice fname secSize = do
  hndl          <- openFile fname ReadWriteMode
  sizeBytes     <- hFileSize hndl
  let numBlocks =  fromIntegral sizeBytes `div` secSize
  return $! Just BlockDevice {
    bdBlockSize  = secSize
  , bdNumBlocks  = numBlocks
  , bdReadBlock  = \ i -> do
                    hSeek hndl AbsoluteSeek (fromIntegral $ i * secSize)
                    v <- BS.hGet hndl secSizeInt
                    let v' = BS.take secSize64 $ v `BS.append` empty
                    return v'
  , bdWriteBlock = \ i v -> do
                    let v' = BS.take secSize64 $ v `BS.append` empty
                    hSeek hndl AbsoluteSeek (fromIntegral $ i * secSize)
                    BS.hPut hndl v'
  , bdFlush      = hFlush hndl
  , bdShutdown   = hClose hndl
  }
 where 
  secSizeI   = fromIntegral secSize
  secSizeInt = fromIntegral secSize
  secSize64  = fromIntegral secSize
  empty      = BS.replicate secSizeI 0
