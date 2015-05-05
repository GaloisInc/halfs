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
import Tests.Types
import Tests.Utils

-- import Debug.Trace

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [ exec 50  "Simple serdes"             propM_blockMapWR
  , exec 50 "In-order alloc/unalloc"     propM_bmInOrderAllocUnallocIntegrity
  , exec 50 "Out-of-order alloc/unalloc" propM_bmOOO
  , exec 50 "Extent aggregation"         propM_bmExtentAggregationIntegrity
  , exec 30 "Serdes stress test"         propM_bmStressWR
  ]
  where
    propM_bmOOO      = propM_bmOutOfOrderAllocUnallocIntegrity (const return)
    propM_bmStressWR = propM_bmOutOfOrderAllocUnallocIntegrity checkBlockMapWR
    --
    exec = mkMemDevExec quick "BlockMap"

--------------------------------------------------------------------------------
-- Property implementations

type BMProp = (Reffable r m, Bitmapped b m, Functor m, Lockable l m) =>
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
        _ -> fail $ "propM_bmAllocUnallocIntegrity: Expected contiguous allocation"
  ------------------------------------------------------------------------------
  -- (1b) Check blockmap state after all allocations are done
    assert =<< isNothing `fmap` run (allocBlocks bm 1)
    checkIntegrity bm 0 [ext] True
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
    checkIntegrity bm (extSz ext) [ext] False
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
  (Reffable r m, Bitmapped b m, Functor m, Lockable l m) =>
     (BlockDevice m -> BlockMap b r l -> PropertyM m (BlockMap b r l))
  -> BDGeom
  -> BlockDevice m
  -> PropertyM m ()
propM_bmOutOfOrderAllocUnallocIntegrity checkBM _g dev = do
  (bm, ext) <- initBM dev
  -- Check integrity over a sequence of block allocations and unallocations.
  -- NB: subSizes is our set of sub-extent sizes that cover the initial free
  -- region.
  forAllM (map extSz `fmap` (permute =<< arbExtents ext)) $ \subSizes -> do
    -- toUnalloc contains BlockGroups allocated by the folded function that
    -- weren't already selected for unallocation
    (bm', toUnalloc) <- foldM (\(bm', allocated) szToAlloc ->
      -- Randomly decide to unallocate a previously-allocated BlockGroup
      forAllM arbitrary $ \(UnallocDecision shouldUnalloc) -> do
        let doUnalloc = shouldUnalloc && not (Prelude.null allocated)
        when doUnalloc $ checkedUnalloc bm' $ head allocated
        blkGroup <- checkedAlloc bm' szToAlloc
        newBM    <- checkBM dev bm'
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
  forAllM (map extSz `fmap` arbExtents ext) $ \subSizes -> case subSizes of
    (large:med:smalls) | (large >= med && med >= sum smalls) -> do
      largeBG  <- checkedAlloc bm large            -- (1) alloc large regions
      medBG    <- checkedAlloc bm med              -- (1) alloc large regions
      smallBGs <- mapM (checkedAlloc bm) smalls    -- (2) extent splitting
      mapM_ (checkedUnalloc bm) smallBGs           -- (3) fragmentation
      aggBG    <- checkedAlloc bm (sum smalls - 1) -- (4) force aggregation
      checkAvail bm 1
      mapM_ (checkedUnalloc bm) [largeBG, medBG, aggBG]
      checkAvail bm (extSz ext)
    _ -> fail "arbExtents failed to yield subextents with expected granularity"

--------------------------------------------------------------------------------
-- Misc helpers

-- | Initialize a new BlockMap and check that internal data structures are
-- coherent
initBM :: (Reffable r m, Bitmapped b m, Lockable l m) =>
          BlockDevice m
       -> PropertyM m (BlockMap b r l, Extent)
initBM dev = do
  bm          <- run $ newBlockMap dev
  initialTree <- run $ readRef $ bmFreeTree bm
  -- NB: ext is the maximally-sized free region for this device
  ext <- case FT.viewl initialTree of
    e FT.:< t' -> assert (FT.null t') >> return e
    FT.EmptyL  -> fail "New blockmap's freetree is empty"
  checkIntegrity bm (extSz ext) [ext] False
  return (bm, ext)

-- | Check that a blockmap is successfully written to and read back from the
-- given device.  Yields the read-back BlockMap (which is required to be the
-- same as the input BlockMap).
checkBlockMapWR :: (Reffable r m, Bitmapped b m, Functor m, Lockable l m) =>
                   BlockDevice m
                -> BlockMap b r l
                -> PropertyM m (BlockMap b r l)
checkBlockMapWR dev bm = do
  run $ writeBlockMap dev bm
  bm' <- run $ readBlockMap dev
  check bm bm'
  return bm'
  where
    assertEq f x y = assert =<< liftM2 (==) (f x) (f y)
    check x y = do
      assertEq (run . numFreeBlocks)      x y
      assertEq (run . toList . bmUsedMap) x y
      -- NB: The free trees most likely won't be identical after read-back, due
      -- to the implicit coalescing that readBlockMap does, so we can't do
      -- assertEq (readRef . bmFreeTree) x y here. Comparing sizes is sufficient
      -- for now, but we might want to eventually use a coalescing function here
      -- to strengthen the check.
      assertEq freeTreeSz x y

-- | Check that (1) the given free block count is the same as reported by the
-- blockmap and the free tree and (2) the given list of extents are all marked
-- as un/used in the usedMap
checkIntegrity :: (Reffable r m, Bitmapped b m, Lockable l m) =>
                  BlockMap b r l -- ^ the blockmap
               -> Word64         -- ^ expected available block count
               -> [Extent]       -- ^ extents to check in the usedmap...
               -> Bool           -- ^ ...to see if they are used (True) or unused
                                 -- (False)
               -> PropertyM m ()
checkIntegrity bm expectAvail exts used = do
  checkAvail bm expectAvail
  assert =<<
    (if used then and else not . or)
    `fmap`
    (run $ mapM (checkBit $ bmUsedMap bm) $ concatMap blkRangeExt exts)

-- | Check that the given free block count is the same as reported by the the
-- blockmap and the free tree
checkAvail :: (Reffable r m, Bitmapped b m, Lockable l m) =>
              BlockMap b r l
           -> Word64
           -> PropertyM m ()
checkAvail bm x = xeq (numFree bm) >> xeq (freeTreeSz bm)
  where xeq y = assert =<< liftM2 (==) y (return x)

-- | Check that a given size can be allocated from the BlockMap and that
-- internal data structures remain coherent
checkedAlloc :: (Reffable r m, Bitmapped b m, Lockable l m) =>
                BlockMap b r l
             -> Word64
             -> PropertyM m BlockGroup
checkedAlloc bm szToAlloc = do
  avail     <- numFree bm
  mblkGroup <- run $ allocBlocks bm szToAlloc
  maybe (fail $ "checkedAlloc: allocation request failed")
        (\blkGroup -> do
          checkIntegrity bm (avail - szToAlloc) (blkGroupExts blkGroup) True
          return blkGroup
        ) mblkGroup

-- | Check that a given BlockGroup can be unallocated from the BlockMap and that
-- internal data structure remain coherent
checkedUnalloc :: (Reffable r m, Bitmapped b m, Lockable l m) =>
                  BlockMap b r l
               -> BlockGroup
               -> PropertyM m ()
checkedUnalloc bm bgToUnalloc = do
  avail <- numFree bm
  run $ unallocBlocks bm bgToUnalloc
  checkIntegrity bm
                 (avail + blkGroupSz bgToUnalloc)
                 (blkGroupExts bgToUnalloc)
                 False

numFree :: (Reffable r m, Lockable l m) =>
           BlockMap b r l -> PropertyM m Word64
numFree = run . readRef . bmNumFree

freeTreeSz :: (Reffable r m, Lockable l m) =>
              BlockMap b r l -> PropertyM m Word64
freeTreeSz bm =
  DF.foldr ((+) . extSz) 0 `fmap` (run $! readRef $! bmFreeTree bm)
