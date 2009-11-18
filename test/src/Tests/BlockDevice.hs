{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tests.BlockDevice
  (
   qcProps
  )
where
  
import Control.Monad (forM_)
import Control.Monad.ST
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Word
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
import System.Directory
import System.IO
  
import System.Device.BlockDevice
import System.Device.File
import System.Device.Memory
import System.Device.ST

import Tests.Instances

--------------------------------------------------------------------------------
-- BlockDevice properties to check

qcProps :: [(Args, Property)]
qcProps =
  [-- File-backed tests 
    ( numTests 10, geomProp "File" fileBackedProp)
  , ( numTests 10, wrProp   "File" fileBackedProp)
   -- Memory-backed tests 
  , ( numTests 50, geomProp "Memory" memBackedProp)
  , ( numTests 50, wrProp   "Memory" memBackedProp)
   -- STArray-backed tests 
  , ( numTests 50, geomProp "STArray" staBackedProp)
  , ( numTests 50, wrProp   "STArray" staBackedProp)
  ]
  where
    numTests n   = stdArgs{maxSuccess = n}
    geomProp s f = labeledIOProp (s ++ "-Backed Block Device Geometry")
                   $ forAllM arbBDGeom (f propM_geom)
    wrProp s f   = labeledIOProp (s ++ "-Backed Block Device Write/Read")
                   $ forAllBlocksM (f . propM_writeRead)

--------------------------------------------------------------------------------
-- Property implementations

-- | Checks that the geometry reported by the given BlockDevice (if any)
-- corresponds to its creation parameters
propM_geom :: Monad m => BDGeom -> BlockDevice m -> PropertyM m ()
propM_geom g dev =
  assert $ bdBlockSize dev == bdgSecSz g &&
           bdNumBlocks dev == bdgSecCnt g   

-- | Checks that blocks written to the Block Device can be read back
-- immediately after write.
propM_writeRead :: Monad m =>
                   [(Word64, ByteString)]
                -> BDGeom
                -> BlockDevice m
                -> PropertyM m ()
propM_writeRead fsData _ dev = do
  forM_ fsData $ \(blkAddr, blkData) -> do 
    run $ bdWriteBlock dev blkAddr blkData
    bs <- run $ bdReadBlock dev blkAddr
    assert $ blkData == bs

--------------------------------------------------------------------------------
-- Utility functions

type PropExec = (BDGeom -> BlockDevice IO -> PropertyM IO ())
             -> BDGeom
             -> PropertyM IO ()
           
-- | Runs the given property on the file-backed block device
fileBackedProp :: PropExec
fileBackedProp f g =
  f g `usingBD` withFileStore g (`newFileBlockDevice` (bdgSecSz g))

-- | Runs the given property on a memory-backed block device
memBackedProp :: PropExec
memBackedProp f g =
  f g `usingBD` newMemoryBlockDevice (bdgSecCnt g) (bdgSecSz g)

-- | Runs the given property on the STArray-backed block device.  This
-- function transforms the underlying ST-based block device an IO block
-- device for interface consistency to top-level properties.
staBackedProp :: PropExec
staBackedProp f g =
  f g `usingBD` stBDToIOBD (newSTBlockDevice (bdgSecCnt g) (bdgSecSz g))

stBDToIOBD :: ST RealWorld (Maybe (BlockDevice (ST RealWorld)))
           -> IO (Maybe (BlockDevice IO))
stBDToIOBD stbd = do
  mdev <- stToIO stbd
  case mdev of
    Nothing     -> return Nothing
    Just oldDev -> return $! Just BlockDevice {
        bdBlockSize  = bdBlockSize oldDev
      , bdNumBlocks  = bdNumBlocks oldDev
      , bdReadBlock  = \i -> stToIO $ bdReadBlock oldDev i
      , bdWriteBlock = \i v -> stToIO $ bdWriteBlock oldDev i v
      , bdFlush      = stToIO $ bdFlush oldDev
      , bdShutdown   = stToIO $ bdShutdown oldDev
      }

usingBD :: Monad m =>
          (BlockDevice m -> PropertyM m ())
       -> m (Maybe (BlockDevice m))
       -> PropertyM m ()
usingBD f act = do
  mdev <- run act
  case mdev of
    Nothing  -> fail "Invalid BlockDevice"
    Just dev -> f dev >> run (bdShutdown dev)

labeledIOProp :: String -> PropertyM IO a -> Property
labeledIOProp s p = label s (monadicIO p)

withFileStore :: BDGeom -> (FilePath -> IO a) -> IO a
withFileStore geom act = do
  let numBytes = fromIntegral (bdgSecSz geom * bdgSecCnt geom)
  (fname, h) <- openTempFile "." "pseudo.dsk"
  BS.hPut h $ BS.replicate numBytes 0
  hClose h
  rslt <- act fname
  removeFile fname
  return rslt
