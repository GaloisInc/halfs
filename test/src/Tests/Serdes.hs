{-# LANGUAGE Rank2Types, FlexibleContexts, ScopedTypeVariables #-}

-- Serialization/deserialization ("serdes") tests

module Tests.Serdes
  (
   qcProps
  )
where
  
import Data.Serialize
import Data.Time.Clock
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
  
import Halfs.Classes
import Halfs.Directory
import Halfs.Inode
import Halfs.SuperBlock

import System.Device.BlockDevice

import Tests.Instances ()
import Tests.Types
import Tests.Utils

import Debug.Trace

--------------------------------------------------------------------------------
-- BlockDevice properties

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [ serdes prop_serdes       100 "SuperBlock"     (arbitrary :: Gen SuperBlock)
  , serdes prop_serdes       100 "UTCTime"        (arbitrary :: Gen UTCTime) 
  , serdes prop_serdes       100 "DirectoryEntry" (arbitrary :: Gen DirectoryEntry)
  , mkMemDevExec quick "Serdes" 100 "Inode" propM_inodeSerdes
  ]
  where
    numTests n      = (,) $ if quick then stdArgs{maxSuccess = n} else stdArgs
    serdes pr n s g = numTests n $ label ("Serdes: " ++ s) $ forAll g pr
    prop_serdes x   = either (const False) (== x) $ decode $ encode x
    
-- We special case inode serdes property because we want to test equality of the
-- inodes' transient fields when possible.  This precludes the use of the pure
-- decode function.
propM_inodeSerdes :: Timed UTCTime m =>
                     BDGeom
                  -> BlockDevice m
                  -> PropertyM m ()
propM_inodeSerdes _g dev = 
  forAllM (arbitrary :: Gen (Inode UTCTime)) $ \inode -> do 
  -- Obtain the expected value inode's numAddrs field post-decoding, based on
  -- dev's geometry instead of the one the inode generator uses, which is
  -- arbitrary.
  nAddrs <- computeNumAddrs (bdBlockSize dev) =<<
            minimalInodeSize (createTime inode)
  run (decodeInode (bdBlockSize dev) (encode inode)) >>=
    assert . either (const False) (== inode { numAddrs = nAddrs })
