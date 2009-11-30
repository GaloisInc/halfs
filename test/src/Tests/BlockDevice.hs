{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.BlockDevice
  (
    qcProps
  )
where
  
import Control.Monad (forM, forM_)
import Control.Monad.Trans
import Data.ByteString (ByteString)
import Data.Word
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
  
import System.Device.BlockDevice

import Tests.Instances
import Tests.Utils

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
                    fileDev (propM_contigWriteRead True)
  , numTests 25 $ wrProp (wrCLabel "Memory-Backed") arbContiguousData
                    memDev (propM_contigWriteRead True)
  , numTests 25 $ wrProp (wrCLabel "STArray-Backed") arbContiguousData
                    staDev (propM_contigWriteRead True)

   -- Single-block write/read properties: rescaled
  , numTests 25 $ swrProp (wrLabel "2x Rescaled") 2 arbFSData propM_writeRead
  , numTests 25 $ swrProp (wrLabel "4x Rescaled") 4 arbFSData propM_writeRead
  , numTests 25 $ swrProp (wrLabel "8x Rescaled") 8 arbFSData propM_writeRead

   -- Contiguous-block write/read properties: rescaled
  , numTests 25 $ swrProp (wrCLabel "2x Rescaled") 2 arbContiguousData
                    (propM_contigWriteRead True)
  , numTests 25 $ swrProp (wrCLabel "4x Rescaled") 4 arbContiguousData
                    (propM_contigWriteRead True)
  , numTests 25 $ swrProp (wrCLabel "8x Rescaled") 8 arbContiguousData
                    (propM_contigWriteRead True)

  -- Single-block write/read properties: cached
  , numTests 50 $ label (wrLabel "Cached") $ monadicBCMIOProp 
    $ forAllBlocksM id arbFSData $ \fsd g ->
      (run $ lift $ memDev g) >>= 
        doProp' (propM_writeRead fsd g . newCachedBlockDevice 512)

  -- Contiguous-block write/read properties: cached
  -- We vary the cache size to trigger eviction at different intervals,
  -- and randomly decide whether or not to flush the cache after writing
  -- a sequence of contiguous blocks.
  , numTests 50 $ label (wrCLabel "Cached") $ monadicBCMIOProp
    $ forAllM (powTwo 1 10)              $ \sz -> 
      forAllM (arbitrary :: Gen Bool)    $ \flush -> 
      forAllBlocksM id arbContiguousData $ \fsd g ->
        (run $ lift $ memDev g) >>=
          doProp' (propM_contigWriteRead flush fsd g
                   . newCachedBlockDevice (fromIntegral sz)
                  )
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

-- | Checks that contiguous blocks written to the Block Device can be
-- read back; when boolean parameter is true, the device is flushed after
-- the write of contiguous blocks.
propM_contigWriteRead :: Bool -> FSDProp m
propM_contigWriteRead flush contigData _g dev = do
  run $ forM_ contigData (uncurry $ bdWriteBlock dev) 
  if flush then run (bdFlush dev) else return ()
  assert . (==) origData =<< run (forM addrs $ bdReadBlock dev)
    where (addrs, origData) = unzip contigData
