{-# LANGUAGE Rank2Types, FlexibleContexts #-}

-- Serialization/deserialization ("serdes") tests

module Tests.Serdes
  (
   qcProps
  )
where
  
import Data.Serialize
import Data.Time.Clock
import Test.QuickCheck hiding (numTests)
  
import Halfs.Inode
import Halfs.SuperBlock

import Tests.Instances ()

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [ serdes 100 "SuperBlock"    (arbitrary :: Gen SuperBlock)
  , serdes 100 "UTCTime"       (arbitrary :: Gen UTCTime) 
  , serdes 100 "Inode UTCTime" (arbitrary :: Gen (Inode UTCTime))
  ]
  where
    numTests n    = (,) $ if quick then stdArgs{maxSuccess = n} else stdArgs
    serdes n s g  = numTests n $ label ("Serdes: " ++ s) $ forAll g prop_serdes
    prop_serdes x = either (const False) (== x) $ decode $ encode x
