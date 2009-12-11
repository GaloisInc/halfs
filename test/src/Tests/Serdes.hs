-- Serialization/deserialization ("serdes") tests
{-# LANGUAGE Rank2Types, FlexibleContexts #-}

module Tests.Serdes
  (
   qcProps
  )
where
  
import Data.Serialize
import Data.Time.Clock
-- import Data.Word
import Test.QuickCheck hiding (numTests)
  
import Halfs.Inode
import Halfs.SuperBlock

import Tests.Instances ()

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [ serdes 100 "SuperBlock"                (arbitrary :: Gen SuperBlock)
  , serdes 100 "UTCTime"                   (arbitrary :: Gen UTCTime) 
  , serdes 100 "Arbitrary (Inode UTCTime)" (arbitrary :: Gen (Inode UTCTime))
  ]
  where
    numTests n   = (,) $ if quick then stdArgs{maxSuccess = n} else stdArgs
    serdes n s g = numTests n $ label ("Serdes: " ++ s) $ forAll g prop_serdes

--------------------------------------------------------------------------------
-- Property implementations

-- | Checks that decode . encode = id for the given input value
prop_serdes :: (Show a, Eq a, Serialize a) => a -> Bool
prop_serdes x = either (const False) (== x) $ decode $ encode x

