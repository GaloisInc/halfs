{-# LANGUAGE Rank2Types, FlexibleContexts, ScopedTypeVariables #-}

-- Serialization/deserialization ("serdes") tests

module Tests.Serdes
  (
   qcProps
  )
where

import Data.Serialize hiding (label)
import Data.Time.Clock
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic

import Halfs.Classes
import Halfs.Directory
import Halfs.HalfsState
import Halfs.Inode
import Halfs.SuperBlock

import System.Device.BlockDevice

import Tests.Instances ()
import Tests.Types
import Tests.Utils

--------------------------------------------------------------------------------
-- BlockDevice properties

-- go = mapM (uncurry quickCheckWithResult) (qcProps True)

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [ serdes prop_serdes 100 "SuperBlock"     (arbitrary :: Gen SuperBlock)
  , serdes prop_serdes 100 "UTCTime"        (arbitrary :: Gen UTCTime)
  , serdes prop_serdes 100 "DirectoryEntry" (arbitrary :: Gen DirectoryEntry)
  , mkMemDevExec quick "Serdes" 100 "Ext"  propM_extSerdes
  , mkMemDevExec quick "Serdes" 100 "Inode" propM_inodeSerdes
  ]
  where
    numTests n      = (,) $ if quick then stdArgs{maxSuccess = n} else stdArgs
    serdes pr n s g = numTests n $ label ("Serdes: " ++ s) $ forAll g pr
    prop_serdes x   = either (const False) (== x) $ decode $ encode x

-- We special case inode serdes property because we want to test equality of the
-- inodes' transient fields when possible.  This precludes the use of the pure
-- decode function.
propM_inodeSerdes :: HalfsCapable b UTCTime r l m =>
                     BDGeom
                  -> BlockDevice m
                  -> PropertyM m ()
propM_inodeSerdes _g dev = forAllM (arbitrary :: Gen (Inode UTCTime)) $ \i -> do
  -- NB: We fill in the numAddrs field of the arbitrary Inode based on the
  -- device geometry rather than using the value given to us by the generator,
  -- which is arbitrary.
  nAddrs <- computeNumAddrs (bdBlockSize dev) minInodeBlocks
              =<< computeMinimalInodeSize (inoCreateTime i)
  sizes  <- run $ computeSizes (bdBlockSize dev)

  let dummyEnv = HalfsState inv inv inv inv sizes inv inv inv inv inv inv inv
      inv      = error "Internal: dummy state, value undefined"

  runH dummyEnv (decodeInode (encode i))
    >>= assert . either (const False) (eq i nAddrs)
  where
    eq inode na = (==) inode{ inoExt = (inoExt inode){ numAddrs = na } }

propM_extSerdes :: HalfsCapable b UTCTime r l m =>
                     BDGeom
                  -> BlockDevice m
                  -> PropertyM m ()
propM_extSerdes _g dev = forAllM (arbitrary :: Gen Ext) $ \ext -> do
  -- NB: We fill in the numAddrs field of the arbitrary Ext here based on the
  -- device geometry rather than using the value given to us by the generator,
  -- which is arbitrary.
  nAddrs <- computeNumAddrs (bdBlockSize dev) minExtBlocks =<< minimalExtSize
  runHNoEnv (decodeExt (bdBlockSize dev) (encode ext))
    >>= assert . either (const False) (== ext{ numAddrs = nAddrs })
