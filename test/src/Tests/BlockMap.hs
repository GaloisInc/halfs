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

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quickMode =
  [
    numTests 25 $ geomProp "BlockMap de/serialization" memDev propM_blockMapWR
  ,
    numTests 100 $ geomProp
                     "BlockMap integrity: in-order alloc/unalloc"
                     memDev
                     propM_bmInOrderAllocUnallocIntegrity
  ,
    numTests 100 $ geomProp
                     "BlockMap integrity: out-of-order alloc/unalloc"
                     memDev
                     (propM_bmOutOfOrderAllocUnallocIntegrity (const return))
  , 
    numTests 100 $ geomProp
                     "BlockMap integrity: alloc/unalloc w/ extent aggregation"
                     memDev
                     propM_bmExtentAggregationIntegrity
--   , (CURRENTLY NOT WORKING)
--     numTests 1 $ geomProp
--                    "BlockMap integrity: BlockMap de/serialization stress test"
--                    memDev
--                    propM_bmStressWR
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

type BMProp = (Reffable r m, Bitmapped b m, Functor m) =>
               BDGeom
            -> BlockDevice m
            -> PropertyM m ()

-- | Ensures that a new blockmap can be written and read back
propM_blockMapWR :: BMProp
propM_blockMapWR _g d = 
  -- write & read-back twice, to ensure there's no baked-in behavior w.r.t the
  -- initial blockmap contents and the de/serialization functions
  fst `fmap` initBM d >>= checkBlockMapWR d >>= checkBlockMapWR d >> return ()
  
-- | Ensures that a new blockmap subjected to a series of (1) random, in-order,
-- contiguous block allocations that entirely cover the free region followed by
-- (2) reverse-order unallocations maintains integrity with respect to free
-- block counts, used map contents, and freetree structure.
propM_bmInOrderAllocUnallocIntegrity :: BMProp
propM_bmInOrderAllocUnallocIntegrity _g dev = do
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
--
-- The first parameter is a blockmap checker (which is permitted to mutate the
-- BlockMap if needed) that is invoked at every allocation step; useful for
-- describing other properties.
propM_bmOutOfOrderAllocUnallocIntegrity ::
  (Reffable r m, Bitmapped b m, Functor m) =>
     (BlockDevice m -> BlockMap b r -> PropertyM m (BlockMap b r))
  -> BDGeom
  -> BlockDevice m
  -> PropertyM m ()
propM_bmOutOfOrderAllocUnallocIntegrity checkBM _g dev = do
  (bm, ext) <- initBM dev
  -- Check integrity over a sequence of block allocations and unallocations.
  -- NB: subSizes is our set of sub-extent sizes that cover the initial free
  -- region.
  forAllM (liftM (map extSz) (permute =<< arbExtents ext)) $ \subSizes -> do
    -- toUnalloc contains BlockGroups allocated by the folded function that
    -- weren't already selected for unallocation
    (bm', toUnalloc) <- foldM (\(bm', allocated) szToAlloc -> do
      -- Randomly decide to unallocate a previously-allocated BlockGroup
      forAllM arbitrary $ \(UnallocDecision shouldUnalloc) -> do
      let doUnalloc = shouldUnalloc && not (Prelude.null allocated)
      when doUnalloc $ checkedUnalloc bm' $ head allocated 
      blkGroup <- checkedAlloc bm' szToAlloc
      newBM <- checkBM dev bm'
      return $ (newBM, blkGroup : (if doUnalloc then tail else id) allocated)
      ) (bm, []) subSizes

    checkAvail bm' (extSz ext - sum (map blkGroupSz toUnalloc))
    mapM_ (checkedUnalloc bm') toUnalloc
    checkAvail bm' (extSz ext)

-- | Ensures that a new blockmap subjected to a series of allocs/unallocs
-- intended to force extent aggregation in the freetree maintains integrity with
-- respect to free block counts, used map contents, and freetree structure.
propM_bmExtentAggregationIntegrity :: BMProp
propM_bmExtentAggregationIntegrity _g dev = do
  -- We can force extent aggregation in the freetree by (e.g.):
  -- (1) Allocating large regions and retaining them
  -- (2) Allocating a number of small regions (results in many split extents)
  -- (3) Unallocating those small regions (fragmentation occurs)
  -- (4) Allocating a region that requires aggregation of small extents
  (bm, ext) <- initBM dev
  forAllM (liftM (map extSz) (arbExtents ext)) $ \subSizes -> do
  case subSizes of
    (large:med:smalls) | (large >= med && med >= sum smalls) -> do
      largeBG  <- checkedAlloc bm large            -- (1) alloc large regions
      medBG    <- checkedAlloc bm med              -- (1) alloc large regions
      smallBGs <- mapM (checkedAlloc bm) smalls    -- (2) extent splitting
      mapM (checkedUnalloc bm) smallBGs            -- (3) fragmentation
      aggBG    <- checkedAlloc bm (sum smalls - 1) -- (4) force aggregation
      checkAvail bm 1
      mapM (checkedUnalloc bm) [largeBG, medBG, aggBG]
      checkAvail bm (extSz ext)
    _ -> fail "arbExtents failed to yield subextents with expected granularity"

-- | Ensures that a blockmap subjected to a variety of allocs/unallocs can be
-- successfully written and read back at every step.
propM_bmStressWR :: BMProp
propM_bmStressWR = propM_bmOutOfOrderAllocUnallocIntegrity checkBlockMapWR
     
--------------------------------------------------------------------------------
-- Misc helpers

-- | Initialize a new BlockMap and check that internal data structures are
-- coherent
initBM :: (Reffable r m, Bitmapped b m) =>
          BlockDevice m
       -> PropertyM m (BlockMap b r, Extent)
initBM dev = do
  bm          <- run $ newBlockMap dev
  initialTree <- run $ readRef $ bmFreeTree bm
  -- NB: ext is the maximally-sized free region for this device
  ext <- case FT.viewl initialTree of
    e FT.:< t' -> assert (FT.null t') >> return e
    FT.EmptyL  -> fail "New blockmap's freetree is empty"
  -- Check initial state
  checkUsedMap bm ext False
  checkAvail bm (extSz ext)
  return (bm, ext)

-- | Check that a blockmap is successfully written to and read back from the
-- given device.  Yields the read-back BlockMap (which is required to be the
-- same as the input BlockMap).
checkBlockMapWR :: (Reffable r m, Bitmapped b m, Functor m) =>
                   BlockDevice m
                -> BlockMap b r
                -> PropertyM m (BlockMap b r)
checkBlockMapWR dev bm = do
  trace ("Writing BM...") $ do
  run $ writeBlockMap dev bm
  trace ("Reading BM...") $ do
  bm' <- run $ readBlockMap dev
  check bm bm'
  trace ("BMs checked OK.") $ do
  return bm'
  where
    assertEq f x y = assert =<< liftM2 (==) (run . f $ x) (run . f $ y)
    check x y = do
      assertEq numFreeBlocks          x y
      assertEq (toList . bmUsedMap)   x y
      assertEq (readRef . bmFreeTree) x y

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

-- | Check that a given size can be allocated from the BlockMap and that
-- internal data structures remain coherent
checkedAlloc :: (Reffable r m, Bitmapped b m) =>
                BlockMap b r
             -> Word64
             -> PropertyM m BlockGroup
checkedAlloc bm szToAlloc = do
  avail     <- numFree bm
  mblkGroup <- run $ allocBlocks bm szToAlloc
  maybe (fail $ "checkedAlloc: allocation request failed")
        (\blkGroup -> do
          checkAvail bm (avail - szToAlloc)
          mapM_ (\ext -> checkUsedMap bm ext True) (blkGroupExts blkGroup)
          return blkGroup
        ) mblkGroup

-- | Check that a given BlockGroup can be unallocated from the BlockMap and that
-- internal data structure remain coherent
checkedUnalloc :: (Reffable r m, Bitmapped b m) =>
                  BlockMap b r
               -> BlockGroup
               -> PropertyM m ()
checkedUnalloc bm bgToUnalloc = do
  trace ("checkedUnalloc start") $ do
  avail <- numFree bm
  run $ unallocBlocks bm bgToUnalloc
  checkAvail bm (avail + blkGroupSz bgToUnalloc)
  mapM_ (\ext -> checkUsedMap bm ext False) (blkGroupExts bgToUnalloc)
  trace ("checkedUnAlloc end") $ return ()

numFree :: Reffable r m => BlockMap b r -> PropertyM m Word64
numFree = run . readRef . bmNumFree 

freeTreeSz :: Reffable r m => BlockMap b r -> PropertyM m Word64
freeTreeSz bm = 
  DF.foldr ((+) . extSz) 0 `fmap` (run $! readRef $! bmFreeTree bm)

