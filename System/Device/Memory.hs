{-# LANGUAGE ScopedTypeVariables, BangPatterns #-}
module System.Device.Memory(
         newMemoryBlockDevice
       )
 where

import Data.Array.IO
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Data.Word

import System.Device.BlockDevice

-- Create a new in-memory block device, with the given number of
-- sectors and sector size.
newMemoryBlockDevice :: Word64 -> Word64 -> IO (Maybe (BlockDevice IO))
newMemoryBlockDevice numSectors sectorSize
  | mostW <= sectorSize = return Nothing
  | otherwise           = do
      arr :: IOArray Word64 ByteString <- newArray (0, numSectors - 1) empty
      return $! Just BlockDevice {
        bdBlockSize  = sectorSize
      , bdNumBlocks  = numSectors
      , bdReadBlock  = \ i -> readArray arr i
      , bdWriteBlock = \ i !v -> do
                        let v' = BS.take secSize64 $ v `BS.append` empty
                        writeArray arr i v'
      , bdFlush      = return ()
      , bdShutdown   = return () -- JS: was 'undefined' for a reason?
      }
 where
  mostW       = fromIntegral (maxBound :: Int)
  secSizeI    = fromIntegral sectorSize
  secSize64   = fromIntegral sectorSize
  empty       = BS.replicate secSizeI 0
