{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE FlexibleContexts #-}

module Tests.BlockMap
  (
   qcProps
  )
where
  
import Control.Monad
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
  
import System.Device.BlockDevice
import Halfs.BlockMap
import Halfs.Classes

import Tests.Instances
import Tests.Utils
  
-- import Debug.Trace

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quickMode =
  [
   -- Geometry properties: basic devices
    numTests 25 $ geomProp "BlockMap de/serialization" memDev propM_blockMapWR
  ]
  where
    numTests n  = (,) $ if quickMode then stdArgs{maxSuccess = n} else stdArgs
    doProp      = (`whenDev` run . bdShutdown)

    geomProp s dev prop = 
      label s $ monadicIO $
      forAllM arbBDGeom $ \g ->
        run (dev g) >>= doProp (prop g)

--------------------------------------------------------------------------------
-- Property implementations

propM_blockMapWR :: (Reffable r m, Bitmapped b m, Functor m, Eq b) =>
                    BDGeom
                 -> BlockDevice m
                 -> PropertyM m ()
propM_blockMapWR _g dev = do
  orig <- run $ newBlockMap dev
  run $ writeBlockMap dev orig
  read1 <- run $ readBlockMap dev
  assertEq numFreeBlocks orig read1
  assertEq (toList . usedMap) orig read1
  -- TODO: Check freetree integrity

  -- rewrite & reread after first read to ensure no baked-in behavior
  -- betwixt initial blockmap and the de/serialization functions
  run $ writeBlockMap dev read1
  read2 <- run $ readBlockMap dev
  assertEq numFreeBlocks read1 read2
  assertEq (toList . usedMap) read1 read2
  -- TODO: Check freetree integrity
  where
    assertEq f x y = assert =<< liftM2 (==) (run . f $ x) (run . f $ y) 
