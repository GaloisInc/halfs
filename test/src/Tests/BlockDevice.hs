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

-- import Debug.Trace

--------------------------------------------------------------------------------

type BDProp m   = BDGeom -> BlockDevice m -> PropertyM m ()
type PropExec m = BDProp m -> BDGeom -> PropertyM m ()

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: [(Args, Property)]
qcProps =
  [
  -- Geometry tests

    (numTests 10, geomProp propM_geomOK
                    (geoLabel "File")   id fileBackedPropExec)
  , (numTests 50, geomProp propM_geomOK
                    (geoLabel "Memory") id memBackedPropExec)
  , (numTests 50, geomProp propM_geomOK
                   (geoLabel "STArray") id staBackedPropExec)

  , (numTests 10, scaledProp (geomProp propM_geomOK)
                    (geoLabel "2x Rescaled-File")    2 fileDev)
  , (numTests 50, scaledProp (geomProp propM_geomOK)
                    (geoLabel "4x Rescaled-STArray") 4 staDev)
  , (numTests 50, scaledProp (geomProp propM_geomOK)
                    (geoLabel "8x Rescaled-Mem")     8 memDev)

  -- Write/read tests

  , (numTests 10, fsDataProp propM_writeRead
                    (wrLabel "File")    id fileBackedPropExec)
  , (numTests 50, fsDataProp propM_writeRead
                    (wrLabel "Memory")  id memBackedPropExec)
  , (numTests 50, fsDataProp propM_writeRead
                    (wrLabel "STArray") id staBackedPropExec)

  , (numTests 10, scaledProp (fsDataProp propM_writeRead)
                    (wrLabel "2x Rescaled-File") 2 fileDev)
  , (numTests 50, scaledProp (fsDataProp propM_writeRead)
                    (wrLabel "4x Rescaled-ST")   4 staDev)
  , (numTests 50, scaledProp (fsDataProp propM_writeRead)
                    (wrLabel "8x Rescaled-Mem")  8 memDev)
  ]
  where
    numTests n           = stdArgs{maxSuccess = n}
    scale k g            = BDGeom (bdgSecCnt g `div` k) (bdgSecSz g * k)
    unscale k g          = BDGeom (bdgSecCnt g * k)     (bdgSecSz g `div` k)
    scaledProp p s k d   = p s (scale k) (rescaledExec k d)
    rescaledExec k d f g = f g `usingBD` rescaledDev (unscale k g) g d
    wrLabel s            = s ++ "-Backed Block Device Write/Read"
    geoLabel s           = s ++ "-Backed Block Device Geometry"

--------------------------------------------------------------------------------
-- Property implementations

-- | Provides the given property with an arbitrary geometry
geomProp :: BDProp IO -> String -> (BDGeom -> BDGeom) -> PropExec IO -> Property
geomProp pr s f exec = labeledIOProp s $ forAllM arbBDGeom (exec pr . f)

-- | Provides the given property with an arbitrary device population and
-- geometry
fsDataProp :: ([(Word64, ByteString)] -> BDProp IO)
           -> String
           -> (BDGeom -> BDGeom)
           -> PropExec IO
           -> Property
fsDataProp pr s f exec = labeledIOProp s $ forAllBlocksM' f (exec . pr)

-- | Checks that the geometry reported by the given BlockDevice (if any)
-- corresponds to its creation parameters
propM_geomOK :: Monad m => BDProp m
propM_geomOK g dev =
  assert $ bdBlockSize dev == bdgSecSz g &&
           bdNumBlocks dev == bdgSecCnt g   

-- | Checks that blocks written to the Block Device can be read back
-- immediately after each is written.
propM_writeRead :: Monad m => [(Word64, ByteString)] -> BDProp m
propM_writeRead fsData _g dev = do
  forM_ fsData $ \(blkAddr, blkData) -> do
    run $ bdWriteBlock dev blkAddr blkData
    run $ bdFlush dev
    bs <- run $ bdReadBlock dev blkAddr
    assert $ blkData == bs

--------------------------------------------------------------------------------
-- Utility functions

type DevCtor = BDGeom -> IO (Maybe (BlockDevice IO))
           
-- | Runs the given property on the file-backed block device
fileBackedPropExec :: PropExec IO
fileBackedPropExec f g = f g `usingBD` fileDev g

-- | Runs the given property on a memory-backed block device
memBackedPropExec :: PropExec IO
memBackedPropExec f g = f g `usingBD` memDev g

-- | Runs the given property on an STArray-backed block device
staBackedPropExec :: PropExec IO
staBackedPropExec f g = f g `usingBD` staDev g

fileDev :: DevCtor
fileDev g = withFileStore g (`newFileBlockDevice` (bdgSecSz g))

memDev :: DevCtor
memDev g = newMemoryBlockDevice (bdgSecCnt g) (bdgSecSz g)

-- | Create an STArray-backed block device.  This function transforms
-- the underlying ST-based block device an IO block device for interface
-- consistency within this module.
staDev :: DevCtor
staDev g = stBDToIOBD $ newSTBlockDevice (bdgSecCnt g) (bdgSecSz g)

rescaledDev :: BDGeom  -- ^ geometry for underlying device
            -> BDGeom  -- ^ new device geometry 
            -> DevCtor -- ^ ctor for underlying device
            -> IO (Maybe (BlockDevice IO))
rescaledDev oldG newG ctor = 
  maybe (fail "Invalid BlockDevice") (newRescaledBlockDevice (bdgSecSz newG))
    `fmap` ctor oldG

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
