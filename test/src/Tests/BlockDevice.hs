{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tests.BlockDevice
  (
   qcProps
  )
where
  
import Control.Monad (foldM, forM, forM_)
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

import Debug.Trace

--------------------------------------------------------------------------------

type BDProp m   = BDGeom -> BlockDevice m -> PropertyM m ()
type PropExec m = BDProp m -> BDGeom -> PropertyM m ()

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quickMode =
  [
  -- Geometry tests: basic devices
    numTests 10 $ gp  "File"    fileBackedPropExec   
  , numTests 25 $ gp  "Memory"  memBackedPropExec
  , numTests 25 $ gp  "STArray" staBackedPropExec
  -- Geometry tests: rescaled devices
  , numTests 10 $ sgp "2x Rescaled-File"    2 fileDev
  , numTests 25 $ sgp "4x Rescaled-STArray" 4 staDev
  , numTests 25 $ sgp "8x Rescaled-Mem"     8 memDev
  -- Filesystem data single-block write/read tests: basic devices
  , numTests 10 $ fdp "File"    fileBackedPropExec
  , numTests 25 $ fdp "Memory"  memBackedPropExec
  , numTests 25 $ fdp "STArray" staBackedPropExec
  -- Filesystem data single-block write/read tests: rescaled devices
  , numTests 10 $ sfdp "2x Rescaled-File" 2 fileDev
  , numTests 25 $ sfdp "4x Rescaled-ST"   4 staDev
  , numTests 25 $ sfdp "8x Rescaled-Mem"  8 memDev
  -- Filesystem data contiguous-block write/read tests: basic devices
  , numTests 10 $ cfdp "File" fileBackedPropExec
  , numTests 25 $ cfdp "Memory" memBackedPropExec
  , numTests 25 $ cfdp "STArray" staBackedPropExec
  -- Filesystem data contiguous-block write/read tests: rescaled devices
  , numTests 10 $ scfdp "2x Rescaled-File" 2 fileDev
  , numTests 25 $ scfdp "4x Rescaled-ST"   4 staDev
  , numTests 25 $ scfdp "8x Rescaled-Mem"  8 memDev
  ]
  where
    numTests n           = (,) $ if quickMode
                                 then stdArgs{maxSuccess = n}
                                 else stdArgs
    scale k g            = BDGeom (bdgSecCnt g `div` k) (bdgSecSz g * k)
    unscale k g          = BDGeom (bdgSecCnt g * k)     (bdgSecSz g `div` k)
    scaledProp p s k d   = p s (scale k) (rescaledExec k d)
    rescaledExec k d f g = f g `usingBD` rescaledDev (unscale k g) g d
    wrLabel s            = s ++ "-Backed Block Device Write/Read"
    wrCLabel s           = s ++ "-Backed Contiguous Block Device Write/Read"
    geoLabel s           = s ++ "-Backed Block Device Geometry"

    -- clutter reduction
    gp s    = geomProp propM_geomOK (geoLabel s) id 
    sgp s   = scaledProp (geomProp propM_geomOK) (geoLabel s)
    fdp s   = fsDataProp propM_writeRead (wrLabel s) id 
    sfdp s  = scaledProp (fsDataProp propM_writeRead) (wrLabel s)
    cfdp s  = fsContigDataProp propM_contigWriteRead (wrCLabel s) id
    scfdp s = scaledProp (fsContigDataProp propM_contigWriteRead) (wrCLabel s)

--------------------------------------------------------------------------------
-- Property implementations

-- | Provides the given property with an arbitrary geometry
geomProp :: BDProp IO -> String -> (BDGeom -> BDGeom) -> PropExec IO -> Property
geomProp pr s f exec =
  labeledIOProp s $ forAllM arbBDGeom (exec pr . f)

-- | Provides the given property with arbitrary geometry and FS data
fsDataProp :: ([(Word64, ByteString)] -> BDProp IO)
           -> String
           -> (BDGeom -> BDGeom)
           -> PropExec IO
           -> Property
fsDataProp pr s f exec =
  labeledIOProp s $ forAllBlocksM' f arbFSData (exec . pr)

-- | Provides the given property with arbitrary geometry and contiguous FS data
fsContigDataProp :: ([(Word64, ByteString)] -> BDProp IO)
                 -> String
                 -> (BDGeom -> BDGeom)
                 -> PropExec IO
                 -> Property
fsContigDataProp pr s f exec =
  labeledIOProp s $ forAllBlocksM' f arbContiguousData (exec . pr)

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
    run $ bdWriteBlock dev blkAddr blkData >> bdFlush dev
    assert . (==) blkData =<< run (bdReadBlock dev blkAddr)

-- | Checks that contiguous blocks written to the Block Device can be read back
propM_contigWriteRead :: Monad m => [(Word64, ByteString)] -> BDProp m
propM_contigWriteRead contigData _g dev = do
  run $ forM_ contigData (uncurry $ bdWriteBlock dev) >> bdFlush dev
  assert . (==) origData =<< run (forM addrs $ bdReadBlock dev)
    where (addrs, origData) = unzip contigData

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
