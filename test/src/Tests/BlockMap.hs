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

import Tests.Instances
import Tests.Utils
  
--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quickMode =
  [
   -- Geometry properties: basic devices
    numTests 10 $ geomProp "BlockMap de/serialization" memDev propM_blockMapWR
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

propM_blockMapWR :: GeomProp m
propM_blockMapWR g dev = assert False
