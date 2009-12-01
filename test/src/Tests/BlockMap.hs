{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE FlexibleContexts #-}

module Tests.BlockMap
  (
   qcProps
  )
where
  
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
    numTests 100 $ geomProp "BlockMap de/serialization" memDev propM_blockMapWR
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
propM_blockMapWR g dev = do
  trace ("propM_blockMapWR: geom = " ++ show g) $ do
  bmap <- run (newBlockMap dev)
--  trace ("propM_blockMapWR: about to write block map") $ do
  run (writeBlockMap dev bmap)
--  trace ("propM_blockMapWR: about to read block map") $ do
  bmap' <- run (readBlockMap dev)
  bnds  <- run (getBounds $ usedMap bmap)
  bnds' <- run (getBounds $ usedMap bmap')
--  trace ("propM_blockMapWR: bnds = " ++ show bnds) $ do
--  trace ("propM_blockMapWR: bnds' = " ++ show bnds') $ do
  bs1 <- run $ mapM (checkBit (usedMap bmap)) [fst bnds..snd bnds]
  bs2 <- run $ mapM (checkBit (usedMap bmap')) [fst bnds'..snd bnds']
--  trace ("propM_blockMapWR: bs1 = " ++ show bs1) $ do
--  trace ("propM_blockMapWR: bs2 = " ++ show bs2) $ do
--  trace ("propM_blockMapWR: eq? " ++ show (bs1 == bs2)) $ do
  assert (bs1 == bs2)
