{-# LANGUAGE ScopedTypeVariables, BangPatterns #-}
module System.Device.ST(
         newSTBlockDevice
       )
 where

import Control.Monad.ST
import Data.Array.ST
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Word

import System.Device.BlockDevice

-- Create a new in-memory block device in ST, with the given number of
-- sectors and sector size.
newSTBlockDevice :: forall s. Word64 -> Word64 -> ST s (Maybe (BlockDevice (ST s)))
newSTBlockDevice numSectors sectorSize
  | mostW <= sectorSize = return Nothing
  | otherwise           = do
      arr <- buildArray
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
  buildArray :: ST s (STArray s Word64 ByteString)
  buildArray  = newArray (0, numSectors - 1) empty
  empty       = BS.replicate secSizeI 0
