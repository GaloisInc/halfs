{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE FlexibleContexts #-}

module Tests.BlockDevice
  (
   qcProps
  )
where
  
import Control.Monad (forM, forM_)
import Control.Monad.Trans
import Control.Monad.ST
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Word
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
import System.Directory
import System.IO
import System.IO.Unsafe (unsafePerformIO)
  
import System.Device.BlockDevice
import System.Device.File
import System.Device.Memory
import System.Device.ST

import Tests.Instances

--------------------------------------------------------------------------------
-- Property types

type GeomProp m = Monad m => BDGeom -> BlockDevice m -> PropertyM m ()
type FSDProp m  = Monad m => [(Word64, ByteString)] -> GeomProp m

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quickMode =
  [
   -- Geometry properties: basic devices
    numTests 10 $ geomProp "File-Backed"    fileDev propM_geomOK
  , numTests 10 $ geomProp "Memory-Backed"  memDev  propM_geomOK
  , numTests 10 $ geomProp "STArray-Backed" staDev  propM_geomOK

   -- Geometry properties: rescaled devices (underlying device is always memDev)
  , numTests 10 $ sgeomProp "2x Rescaled" 2 propM_geomOK
  , numTests 10 $ sgeomProp "4x Rescaled" 4 propM_geomOK
  , numTests 10 $ sgeomProp "8x Rescaled" 8 propM_geomOK

   -- Geometry properties: cached device
  , numTests 10 $ label (geoLabel "Cached") $ monadicBCMIOProp $
    forAllM arbBDGeom $ \g ->
      (run $ lift $ memDev g) >>=
        doProp' (propM_geomOK g . newCachedBlockDevice 512)

   -- Single-block write/read properties: basic devices
  , numTests 10 $ wrProp (wrLabel "File-Backed") arbFSData
                    fileDev propM_writeRead 
  , numTests 25 $ wrProp (wrLabel "Memory-Backed") arbFSData
                    memDev  propM_writeRead
  , numTests 25 $ wrProp (wrLabel "STArray-Backed") arbFSData
                    staDev  propM_writeRead

   -- Contiguous-block write/read properties: basic devices
  , numTests 10 $ wrProp (wrCLabel "File-Backed") arbContiguousData
                    fileDev propM_contigWriteRead
  , numTests 25 $ wrProp (wrCLabel "Memory-Backed") arbContiguousData
                    memDev propM_contigWriteRead
  , numTests 25 $ wrProp (wrCLabel "STArray-Backed") arbContiguousData
                    staDev propM_contigWriteRead

   -- Single-block write/read properties: rescaled
  , numTests 25 $ swrProp (wrLabel "2x Rescaled") 2 arbFSData propM_writeRead
  , numTests 25 $ swrProp (wrLabel "4x Rescaled") 4 arbFSData propM_writeRead
  , numTests 25 $ swrProp (wrLabel "8x Rescaled") 8 arbFSData propM_writeRead

   -- Contiguous-block write/read properties: rescaled
  , numTests 25 $ swrProp (wrCLabel "2x Rescaled") 2 arbContiguousData
                    propM_contigWriteRead
  , numTests 25 $ swrProp (wrCLabel "4x Rescaled") 4 arbContiguousData
                    propM_contigWriteRead
  , numTests 25 $ swrProp (wrCLabel "8x Rescaled") 8 arbContiguousData
                    propM_contigWriteRead

  -- Single-block write/read properties: cached
  , numTests 25 $ label (wrLabel "Cached") $ monadicBCMIOProp 
    $ forAllBlocksM id arbFSData $ \fsd g ->
      (run $ lift $ memDev g) >>= 
        doProp' (propM_writeRead fsd g . newCachedBlockDevice 512)

  -- Contiguous-block write/read properties: cached
  -- We use a minimal cache size here to trigger frequent eviction
  , numTests 25 $ label (wrCLabel "Cached") $ monadicBCMIOProp
    $ forAllBlocksM id arbContiguousData $ \fsd g ->
      (run $ lift $ memDev g) >>=
        doProp' (propM_contigWriteRead fsd g . newCachedBlockDevice 2)
  ]
  where
    numTests n  = (,) $ if quickMode then stdArgs{maxSuccess = n} else stdArgs
    scale k g   = BDGeom (bdgSecCnt g `div` k) (bdgSecSz g * k)
    unscale k g = BDGeom (bdgSecCnt g * k)     (bdgSecSz g `div` k)
    geoLabel s  = s ++ " Block Device Geometry"
    wrLabel s   = s ++ " Single Block Write/Read"
    wrCLabel s  = s ++ " Contiguous Block Write/Read"
    doProp      = (`whenDev` run . bdShutdown)
    doProp'     = (`whenDev` run . lift . bdShutdown)

    geomProp s dev prop = 
      label (geoLabel s) $ monadicIO $
      forAllM arbBDGeom $ \g ->
        run (dev g) >>= doProp (prop g)

    sgeomProp s k prop = -- scaled variant: always uses memDev
      label (geoLabel s) $ monadicIO $
      forAllM arbBDGeom $ \g ->
        run (rescaledDev (unscale k g) g memDev) >>= doProp (prop g)

    wrProp s gen dev prop =
      label s $ monadicIO $ forAllBlocksM id gen $ \fsd g ->
        run (dev g) >>= doProp (prop fsd g) 

    swrProp s k gen prop = -- scaled variant: always uses memdev
      label s $ monadicIO $ forAllBlocksM (scale k) gen $ \fsd g ->
        run (rescaledDev (unscale k g) g memDev) >>= doProp (prop fsd g)

--------------------------------------------------------------------------------
-- Property implementations

-- | Checks that the geometry reported by the given BlockDevice (if any)
-- corresponds to its creation parameters
propM_geomOK :: GeomProp m
propM_geomOK g dev =
  assert $ bdBlockSize dev == bdgSecSz g &&
           bdNumBlocks dev == bdgSecCnt g   

-- | Checks that blocks written to the Block Device can be read back
-- immediately after each is written.
propM_writeRead :: FSDProp m
propM_writeRead fsData _g dev = do
  forM_ fsData $ \(blkAddr, blkData) -> do
    run $ bdWriteBlock dev blkAddr blkData >> bdFlush dev
    assert . (==) blkData =<< run (bdReadBlock dev blkAddr)

-- | Checks that contiguous blocks written to the Block Device can be read back
propM_contigWriteRead :: FSDProp m
propM_contigWriteRead contigData _g dev = do
  run $ forM_ contigData (uncurry $ bdWriteBlock dev) >> bdFlush dev
  assert . (==) origData =<< run (forM addrs $ bdReadBlock dev)
    where (addrs, origData) = unzip contigData

--------------------------------------------------------------------------------
-- Utility functions

type DevCtor = BDGeom -> IO (Maybe (BlockDevice IO))
           
fileDev :: DevCtor
fileDev g = withFileStore g (`newFileBlockDevice` (bdgSecSz g))

memDev :: DevCtor
memDev g = newMemoryBlockDevice (bdgSecCnt g) (bdgSecSz g)

-- | Create an STArray-backed block device.  This function transforms
-- the ST-based block device to an IO block device for interface
-- consistency within this module.
staDev :: DevCtor
staDev g =
  stToIO (newSTBlockDevice (bdgSecCnt g) (bdgSecSz g)) >>= 
  return . maybe Nothing (\dev ->
    Just BlockDevice {
        bdBlockSize = bdBlockSize dev
      , bdNumBlocks = bdNumBlocks dev
      , bdReadBlock  = \i   -> stToIO $ bdReadBlock dev i
      , bdWriteBlock = \i v -> stToIO $ bdWriteBlock dev i v
      , bdFlush      = stToIO $ bdFlush dev
      , bdShutdown   = stToIO $ bdShutdown dev
    })

rescaledDev :: BDGeom  -- ^ geometry for underlying device
            -> BDGeom  -- ^ new device geometry 
            -> DevCtor -- ^ ctor for underlying device
            -> IO (Maybe (BlockDevice IO))
rescaledDev oldG newG ctor = 
  maybe (fail "Invalid BlockDevice") (newRescaledBlockDevice (bdgSecSz newG))
    `fmap` ctor oldG

monadicBCMIOProp :: PropertyM (BCM IO) a -> Property
monadicBCMIOProp = monadic (unsafePerformIO . runBCM)

withFileStore :: BDGeom -> (FilePath -> IO a) -> IO a
withFileStore geom act = do
  let numBytes = fromIntegral (bdgSecSz geom * bdgSecCnt geom)
  (fname, h) <- openTempFile "." "pseudo.dsk"
  BS.hPut h $ BS.replicate numBytes 0
  hClose h
  rslt <- act fname
  removeFile fname
  return rslt

whenDev :: (Monad m) => (a -> m b) -> (a -> m ()) -> Maybe a -> m b
whenDev act cleanup =
  maybe (fail "Invalid BlockDevice") $ \x -> do
    y <- act x
    cleanup x
    return y
