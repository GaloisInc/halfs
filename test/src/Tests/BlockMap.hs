{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.BlockMap
  (
   qcProps
  )
where
  
import Control.Monad
import Data.FingerTree
import Data.Maybe (isNothing)
import Prelude hiding (null)
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
--    numTests 25 $ geomProp "BlockMap de/serialization" memDev propM_blockMapWR
--  ,
    numTests 50 $ geomProp
                    "BlockMap internal integrity under contiguous un/allocation"
                    memDev
                    propM_bmInOrderAllocUnallocIntegrity
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

-- | Ensures that a new blockmap can be written and read back
propM_blockMapWR :: (Reffable r m, Bitmapped b m, Functor m) =>
                    BDGeom
                 -> BlockDevice m
                 -> PropertyM m ()
propM_blockMapWR _g dev = do
--  trace ("propM_blockMapWR: g = " ++ show g) $ do
  orig <- run $ newBlockMap dev
  run $ writeBlockMap dev orig
  read1 <- run $ readBlockMap dev
  check orig read1

  -- rewrite & reread after first read to ensure no baked-in behavior
  -- betwixt initial blockmap and the de/serialization functions
  run $ writeBlockMap dev read1
  read2 <- run $ readBlockMap dev
  check read1 read2

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
    check x y = do
      assertEq numFreeBlocks          x y
      assertEq (toList . bmUsedMap)   x y
      assertEq (readRef . bmFreeTree) x y

-- | Ensures that a new blockmap subjected to a series of (1) in-order
-- contiguous block allocations followed by (2) in-order unallocations maintains
-- integrity with respect to free block counts, used map contents, and freetree
-- structure.
propM_bmInOrderAllocUnallocIntegrity ::
  (Reffable r m, Bitmapped b m, Functor m) =>
     BDGeom
  -> BlockDevice m
  -> PropertyM m ()
propM_bmInOrderAllocUnallocIntegrity _g dev = do
--  trace ("propM_bmAllocUnallocIntegrity: g = " ++ show g) $ do
  bm          <- run $ newBlockMap dev
  avail       <- numFree bm
  initialTree <- readTree bm
  ext         <- case viewl initialTree of
    e :< t' -> assert (null t') >> return e
    EmptyL  -> assert False     >> fail "New blockmap's freetree is empty"

  -- NB: ext is the maximally-sized free region for this device

  checkUsedMap bm ext False   -- Used map is entirely marked 'unused'
  checkAvail bm (extSz ext)

  -- Check integrity over sequence of contiguous block allocations followed by
  -- sequence of corresponding unallocations.  NB: subs is our set of
  -- sub-extents that cover the initial free region
  forAllM (arbExtents ext) $ \subs -> do

  ------------------------------------------------------------------------------
  -- (1a) Check integrity during sequence of contiguous block allocations

  forM_ (subs `zip` availSeq (-) avail subs) $ \(sub, avail') -> do
    Just blks <- run $ allocBlocks bm (extSz sub)
    assert (head blks == extBase sub)                -- got expected base
    assert (fromIntegral (length blks) == extSz sub) -- got expected size
    checkUsedMap bm sub True
    checkAvail bm avail'

  ------------------------------------------------------------------------------
  -- (1b) After all allocations, check that an additional allocation fails and
  --      used map is entirely marked 'used'.
  assert =<< isNothing `fmap` run (allocBlocks bm 1)
  checkUsedMap bm ext True

  --------------------------------------------------------------------------------
  -- (2a) Check integrity during sequence of contiguous block deallocations (NB:
  --      we unallocate in most-recently-allocated order to keep freetree
  --      perturbations minimal in this particular property)

  let rsubs = Prelude.reverse subs 
  forM_ (rsubs `zip` availSeq (+) 0 rsubs) $ \(sub, avail') -> do
    run $ unallocBlocksContig bm (extBase sub) (extBase sub + extSz sub - 1)
    checkUsedMap bm sub False
    checkAvail bm avail'

  -- (2b) After all unallocations, check that used map is entirely marked
  -- 'unused' and an additional allocation succeeds.
  checkUsedMap bm ext False
  Just _ <- run $ allocBlocks bm 1 
  return ()
  where
    availSeq op x   = drop 1 . scanl op x . map extSz
    readTree        = run . readRef . bmFreeTree
    numFree         = run . readRef . bmNumFree 
    -- Check that the given free block count is the same as the blockmap reports
    checkAvail bm x = assert =<< liftM2 (==) (numFree bm) (return x)
    -- Check that the given extent is marked as un/used in the usedMap
    checkUsedMap bm ext used =
      assert =<< liftM (if used then and else not . or)
                       (run $ mapM (checkBit $ bmUsedMap bm) (blkRangeExt ext))



  



