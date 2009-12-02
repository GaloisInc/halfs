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

{-
  -- temp
  t1 <- run $ readRef $ bmFreeTree orig
  trace ("t1 = " ++ show t1) $ do
  Just blks <- run $ allocBlocks orig 50
  trace ("blks = " ++ show blks) $ do  
  t2 <- run $ readRef $ bmFreeTree orig
  trace ("t2 = " ++ show t2) $ do
  usedMap <- run $ toList $ bmUsedMap orig
  trace ("usedMap = " ++ show usedMap) $ do

  trace ("unallocing [3,4]") $ do
  run $ unallocBlocksContig orig 3 4
  t3 <- run $ readRef $ bmFreeTree orig
  trace ("t3 = " ++ show t3) $ do

  trace ("unallocing [16..25]") $ do
  run $ unallocBlocksContig orig 16 25
  t4 <- run $ readRef $ bmFreeTree orig
  trace ("t4 = " ++ show t4) $ do

  Just blks2 <- run $ allocBlocks orig 11
  trace ("blks2 = " ++ show blks2) $ do  
  t5 <- run $ readRef $ bmFreeTree orig
  trace ("t5 = " ++ show t5) $ do

  Just blks3 <- run $ allocBlocks orig 11
  trace ("blks3 = " ++ show blks3) $ do  
  t6 <- run $ readRef $ bmFreeTree orig
  trace ("t6 = " ++ show t6) $ do

  trace ("unallocing " ++ show blks3) $ do
  run $ unallocBlocks orig blks3
  t7 <- run $ readRef $ bmFreeTree orig
  trace ("t7 = " ++ show t7) $ do

  assert True
  -- temp
-}

  where
    assertEq f x y = assert =<< liftM2 (==) (run . f $ x) (run . f $ y) 
