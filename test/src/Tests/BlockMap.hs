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
  
import Debug.Trace

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quickMode =
  [
   -- Geometry properties: basic devices
    numTests 1 $ geomProp "BlockMap de/serialization" memDev propM_blockMapWR
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

propM_blockMapWR :: (Reffable r m, Bitmapped b m, Functor m) =>
                    BDGeom
                 -> BlockDevice m
                 -> PropertyM m ()
propM_blockMapWR g dev = do
  trace ("propM_blockMapWR: g = " ++ show g) $ do
  orig <- run $ newBlockMap dev
  run $ writeBlockMap dev orig
  read1 <- run $ readBlockMap dev
  assertEq numFreeBlocks          orig read1
  assertEq (toList . bmUsedMap)   orig read1
  assertEq (readRef . bmFreeTree) orig read1

  -- rewrite & reread after first read to ensure no baked-in behavior
  -- betwixt initial blockmap and the de/serialization functions
  run $ writeBlockMap dev read1
  read2 <- run $ readBlockMap dev
  assertEq numFreeBlocks          read1 read2
  assertEq (toList . bmUsedMap)   read1 read2
  assertEq (readRef . bmFreeTree) read1 read2

  -- temp
  t1 <- run $ readRef $ bmFreeTree orig
  trace ("t1 = " ++ show t1) $ do
  blks <- run $ allocBlocks orig 31
  trace ("blks = " ++ show blks) $ do  
  t2 <- run $ readRef $ bmFreeTree orig
  trace ("t2 = " ++ show t2) $ do
  assert True
  -- temp

  where
    assertEq f x y = assert =<< liftM2 (==) (run . f $ x) (run . f $ y) 
