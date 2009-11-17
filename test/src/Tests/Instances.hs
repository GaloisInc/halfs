{-# OPTIONS_GHC -fno-warn-orphans #-}

module Tests.Instances
where

import Control.Exception         (assert)
import Data.Bits                 (shiftL)
import Data.Word
import Control.Applicative       ((<$>), (<*>))
import Test.QuickCheck

data BDGeom = BDGeom
  { bdgSecCnt :: Word64 -- number of sectors
  , bdgSecSz  :: Word64 -- sector size, in bytes
  } deriving Show

-- | Generate a power of 2 given an exponent range in [0, 63]
powTwo :: Int -> Int -> Gen Word64
powTwo l h = assert (l >= 0 && h <= 63) $ shiftL 1 <$> choose (l, h)

instance Arbitrary BDGeom where
  arbitrary = BDGeom
              <$> powTwo 10 13 -- 1024..8192 sectors
              <*> powTwo  8 12 -- 256b..4K sector size
              -- => 256K .. 32M filesystem size

arbBDGeom :: Gen BDGeom
arbBDGeom = arbitrary
