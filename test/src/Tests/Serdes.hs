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
import Halfs.Protection
import Halfs.SuperBlock

import Tests.Instances ()
-- import Debug.Trace

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [  numTests 100 $ forAll (arbitrary :: Gen SuperBlock) prop_serdes
  -- vvv currently failing due to UTCTime serdes bug
  -- See Arbitrary instance for DiffTime in Tests.Instances and Serialization
  -- instance for UTCTime in Halfs.Classes for more details
  , numTests 1 $ forAll (arbitrary :: Gen UTCTime) prop_serdes
  , numTests 1 $ forAll (arbitrary :: Gen UTCTime) $ \t ->
      -- check serdes for an empty inode (sanity check)
      prop_serdes $ emptyInode
                      (fromIntegral minimumNumberOfBlocks)
                      t
                      t
                      nilInodeRef
                      nilInodeRef
                      rootUser
                      rootGroup
  , numTests 1 $ forAll (arbitrary :: Gen (Inode UTCTime)) prop_serdes
  ]
  where
    numTests n = (,) $ if quick then stdArgs{maxSuccess = n} else stdArgs

--------------------------------------------------------------------------------
-- Property implementations

-- | Checks that decode . encode = id for the given input value
prop_serdes :: (Show a, Eq a, Serialize a) => a -> Bool
prop_serdes x = -- either (const False) (== x) $ decode $ encode x
  let encoded = encode x
      decoded = decode encoded
  in
--    trace ("x       = " ++ show x)       $
--    trace ("decoded = " ++ show decoded) $
--    trace ("encoded = " ++ show encoded) $
      either (const False) (== x) decoded

