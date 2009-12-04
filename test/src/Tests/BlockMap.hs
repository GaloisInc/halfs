{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.BlockMap
  (
   qcProps
  )
where
  
import Control.Monad
import qualified Data.FingerTree as FT
import qualified Data.Foldable as DF
import Data.Maybe (isNothing, isJust)
import Data.Word
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
  
import System.Device.BlockDevice
import Halfs.BlockMap
import Halfs.Classes

import Tests.Instances
import Tests.Utils
  
import Debug.Trace
import Data.List (sort)

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quickMode =
  [
--     numTests 25 $ geomProp "BlockMap de/serialization" memDev propM_blockMapWR
--  ,
--     numTests 100 $ geomProp
--                     "BlockMap integrity: in-order alloc/unalloc"
--                     memDev
--                     propM_bmInOrderAllocUnallocIntegrity
--  ,
     numTests 1 $ geomProp
                    "BlockMap integrity: out-of-order alloc/unalloc"
                    memDev
                    propM_bmOutOfOrderAllocUnallocIntegrity
  , 
     numTests 1 $ geomProp
                   "BlockMap integrity: alloc/unalloc w/ extent aggregation"
                   memDev
                   propM_bmExtentAggregationIntegrity
  ]
  where
    numTests n  = (,) $ if quickMode then stdArgs{maxSuccess = n} else stdArgs
    doProp      = (`whenDev` run . bdShutdown)
    -- 
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
  where
    assertEq f x y = assert =<< liftM2 (==) (run . f $ x) (run . f $ y)
    check x y = do
      assertEq numFreeBlocks          x y
      assertEq (toList . bmUsedMap)   x y
      assertEq (readRef . bmFreeTree) x y

-- | Ensures that a new blockmap subjected to a series of (1) random, in-order,
-- contiguous block allocations that entirely cover the free region followed by
-- (2) reverse-order unallocations maintains integrity with respect to free
-- block counts, used map contents, and freetree structure.
propM_bmInOrderAllocUnallocIntegrity ::
  (Reffable r m, Bitmapped b m, Functor m) =>
     BDGeom
  -> BlockDevice m
  -> PropertyM m ()
propM_bmInOrderAllocUnallocIntegrity _g dev = do
--  trace ("propM_bmAllocUnallocIntegrity: g = " ++ show g) $ do
  (bm, ext) <- initBM dev
  ------------------------------------------------------------------------------
  -- Check integrity over sequence of contiguous block allocations followed by
  -- sequence of corresponding unallocations.  NB: subs is our set of
  -- sub-extents that cover the initial free region
  forAllM (arbExtents ext) $ \subs -> do
  ------------------------------------------------------------------------------
  -- (1a) Check integrity during sequence of contiguous block allocations
  forM_ subs $ \sub -> do
    blkGrp <- checkedAlloc bm (extSz sub)
    case blkGrp of
      Contig allocExt ->
        assert (allocExt == sub) -- got expected base & size; this only works
                                 -- because we are doing in-order contiguous
                                 -- allocation
      _ ->
        fail $ "propM_bmAllocUnallocIntegrity: Expected contiguous allocation"
  ------------------------------------------------------------------------------
  -- (1b) Check blockmap state after all allocations are done
  assert =<< isNothing `fmap` run (allocBlocks bm 1)
  checkAvail bm 0
  checkUsedMap bm ext True
  ------------------------------------------------------------------------------
  -- (2a) Check integrity during sequence of contiguous block deallocations. NB:
  --      we unallocate in most-recently-allocated order to reduce freetree
  --      perturbations in this simple property.  If we haven't failed before
  --      now, we know that all subextents were contiguously allocated, so we
  --      don't have to directly track the return value of prior checkedAlloc
  --      invocations in order to supply the correct BlockGroup to
  --      checkedUnalloc
  forM_ (Prelude.reverse subs) $ checkedUnalloc bm . Contig
  ------------------------------------------------------------------------------ 
  -- (2b) Check blockmap state after all unallocations are done
  checkAvail bm (extSz ext)
  checkUsedMap bm ext False
  assert =<< isJust `fmap` run (allocBlocks bm 1)

-- | Ensures that a new blockmap subjected to a series of (1) random,
-- out-of-order, block allocations intermixed with (2) potentially out-of-order
-- unallocations maintains integrity with respect to free block counts, used map
-- contents, and freetree structure.
propM_bmOutOfOrderAllocUnallocIntegrity ::
  (Reffable r m, Bitmapped b m, Functor m) =>
     BDGeom
  -> BlockDevice m
  -> PropertyM m ()
propM_bmOutOfOrderAllocUnallocIntegrity _g dev = do
  (bm, ext) <- initBM dev
  -- Check integrity over a sequence of (possibly discontiguous) block
  -- allocations and unallocations.  NB: subSizes is our set of sub-extent sizes
  -- that cover the initial free region.
  forAllM (liftM (map extSz) (permute =<< arbExtents ext)) $ \subSizes -> do
--    trace ("subSizes = " ++ show subSizes) $ do
    toUnalloc <- foldM (\allocated szToAlloc -> do
--      trace ("=== oooAllocUnalloc ===") $ do
--      trace ("szToAlloc = " ++ show szToAlloc) $ do
--      trace ("allocated = " ++ show allocated) $ do
      -- Randomly decide to unallocate a previously-allocated block group
      forAllM arbitrary $ \(UnallocDecision shouldUnalloc) -> do
        let doUnalloc = shouldUnalloc && not (Prelude.null allocated)
        when doUnalloc $ checkedUnalloc bm $ head allocated 
        blkGroup <- checkedAlloc bm szToAlloc
        return $ blkGroup : (if doUnalloc then tail else id) allocated
      ) [] subSizes

    assert False
    checkAvail bm (extSz ext - sum (map blkGroupSz toUnalloc))
    mapM_ (checkedUnalloc bm) toUnalloc
    checkAvail bm (extSz ext)

-- | Esnures that a new blockmap subjected to a series of allocs/unallocs
-- intended to force extend aggregation maintains integrity with respect to free
-- block counts, used map contents, and freetree structure.  

propM_bmExtentAggregationIntegrity ::
  (Reffable r m, Bitmapped b m, Functor m) =>
     BDGeom
  -> BlockDevice m
  -> PropertyM m ()
propM_bmExtentAggregationIntegrity g dev = do
  -- We can force extent aggregation by (e.g.):
  -- (1) Allocating large regions and retaining them
  -- (2) Allocating a number of small regions (results in many split extents)
  -- (3) Unallocating those small regions (fragmentation occurs)
  -- (4) Allocating a region that requires aggregation of small extents

  trace ("propM_bmExtentAggregationIntegrity: g = " ++ show g) $ do

  (bm, ext) <- initBM dev
  forAllM (liftM (map extSz) (arbExtents ext)) $ \subSizes -> do
  case subSizes of
    (large:med:smalls) | (large >= med && med >= sum smalls) -> do
      return ()
    _ -> fail "arbExtents failed to return subextents with expected granularity"

--------------------------------------------------------------------------------
-- Misc helpers

availSeq :: (a -> Word64 -> a) -> a -> [Extent] -> [a]
availSeq op x = drop 1 . scanl op x . map extSz

-- | Check that the given free block count is the same as reported by the the
-- blockmap and the free tree
checkAvail :: (Reffable r m, Bitmapped b m) => BlockMap b r -> Word64 -> PropertyM m ()
checkAvail bm x = xeq (numFree bm) >> xeq (freeTreeSz bm)
  where xeq y = assert =<< liftM2 (==) y (return x)

-- Check that the given extent is marked as un/used in the usedMap
checkUsedMap :: Bitmapped b m =>
                BlockMap b r
             -> Extent
             -> Bool
             -> PropertyM m ()
checkUsedMap bm ext used =
  assert =<< liftM (if used then and else not . or)
                   (run $ mapM (checkBit $ bmUsedMap bm) (blkRangeExt ext))

checkedAlloc :: (Reffable r m, Bitmapped b m) =>
                BlockMap b r
             -> Word64
             -> PropertyM m BlockGroup
checkedAlloc bm szToAlloc = do
--   trace ("Doing checkedAlloc: sz = " ++ show szToAlloc) $ do
  avail     <- numFree bm
  mblkGroup <- run $ allocBlocks bm szToAlloc
  maybe (fail $ "checkedAlloc: allocation request failed")
        (\blkGroup -> do
          checkAvail bm (avail - szToAlloc)
          mapM_ (\ext -> checkUsedMap bm ext True) (blkGroupExts blkGroup)
          return blkGroup
        ) mblkGroup

checkedUnalloc :: (Reffable r m, Bitmapped b m) =>
                  BlockMap b r
               -> BlockGroup
               -> PropertyM m ()
checkedUnalloc bm bgToUnalloc = do
--   trace ("Doing checkedUnalloc: bgToUnalloc = " ++ show bgToUnalloc) $ do
  avail <- numFree bm
  run $ unallocBlocks bm bgToUnalloc
  checkAvail bm (avail + blkGroupSz bgToUnalloc)
  mapM_ (\ext -> checkUsedMap bm ext False) (blkGroupExts bgToUnalloc)

numFree :: Reffable r m => BlockMap b r -> PropertyM m Word64
numFree = run . readRef . bmNumFree 

freeTreeSz :: Reffable r m => BlockMap b r -> PropertyM m Word64
freeTreeSz bm = 
  DF.foldr (\e -> (extSz e +)) 0 `fmap` (run $! readRef $! bmFreeTree bm)

initBM :: (Reffable r m, Bitmapped b m) =>
          BlockDevice m
       -> PropertyM m (BlockMap b r, Extent)
initBM dev = do
  bm          <- run $ newBlockMap dev
  initialTree <- run $ readRef $ bmFreeTree bm
  -- NB: ext is the maximally-sized free region for this device
  ext         <- case FT.viewl initialTree of
    e FT.:< t' -> assert (FT.null t') >> return e
    FT.EmptyL  -> fail "New blockmap's freetree is empty"
  -- Check initial state
  checkUsedMap bm ext False
  checkAvail bm (extSz ext)
  return (bm, ext)
