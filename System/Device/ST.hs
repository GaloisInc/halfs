{-# LANGUAGE ScopedTypeVariables #-}
module System.Device.ST(
         newSTBlockDevice
       )
 where

import Control.Monad.ST
import Data.Array.ST
import Data.Array.MArray
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Data.Word

import System.Device.BlockDevice

-- Create a new in-memory block device in ST, with the given number of
-- sectors and sector size.
newSTBlockDevice :: Word64 -> Word64 -> ST s (Maybe (BlockDevice (ST s)))
newSTBlockDevice numSectors sectorSize
  | mostW > sectorSize = return Nothing
  | otherwise          = do
      arr <- buildArray
      return $! Just BlockDevice {
        bdBlockSize  = sectorSize
      , bdNumBlocks  = numSectors
      , bdReadBlock  = \ i -> readArray arr i
      , bdWriteBlock = \ i v -> do
                        let v' = BS.take secSize64 $ v `BS.append` empty
                        writeArray arr i v'
      , bdFlush      = return ()
      , bdShutdown   = undefined
      }
 where
  most :: Int = maxBound
  mostW       = fromIntegral most
  secSizeI    = fromIntegral sectorSize
  secSize64   = fromIntegral sectorSize
  buildArray :: ST s (STArray s Word64 ByteString)
  buildArray  = newArray (0, numSectors - 1) empty -- AARGH.
  empty       = BS.replicate secSizeI 0

