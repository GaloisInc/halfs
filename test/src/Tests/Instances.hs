{-# OPTIONS_GHC -fno-warn-orphans #-}

module Tests.Instances
where

import Control.Exception         (assert)
import Control.Monad             (replicateM)
import Data.Bits                 (shiftL)
import Data.ByteString           (ByteString)
import qualified Data.ByteString as BS
import Data.Word
import Control.Applicative       ((<$>), (<*>))
import Test.QuickCheck
import Test.QuickCheck.Monadic hiding (assert)

data BDGeom = BDGeom
  { bdgSecCnt :: Word64       -- ^ number of sectors
  , bdgSecSz  :: Word64       -- ^ sector size, in bytes
  } deriving Show

-- | Generate a power of 2 given an exponent range in [0, 63]
powTwo :: Int -> Int -> Gen Word64
powTwo l h = assert (l >= 0 && h <= 63) $ shiftL 1 <$> choose (l, h)

instance Arbitrary BDGeom where
  arbitrary = BDGeom
              <$> powTwo 10 13   -- 1024..8192 sectors
              <*> powTwo  8 12   -- 256b..4K sector size
              -- => 256K .. 32M filesystem size

forAllBlocksM :: Monad m =>
                 ([(Word64, ByteString)] -> BDGeom -> PropertyM m b)
              -> PropertyM m b
forAllBlocksM prop = forAllBlocksM' id prop

forAllBlocksM' :: Monad m =>
                  (BDGeom -> BDGeom)
               -> ([(Word64, ByteString)] -> BDGeom -> PropertyM m b)
               -> PropertyM m b
forAllBlocksM' f prop =
  forAllM arbBDGeom $ \g ->
    let g' = f g in
      forAllM (arbFSData g') (flip prop g')

arbBDGeom :: Gen BDGeom
arbBDGeom = arbitrary

arbFSData :: BDGeom -> Gen [(Word64, ByteString)]
arbFSData g = listOf1 $ (,) <$> arbBlockAddr g <*> arbBlockData g

arbBlockAddr :: BDGeom -> Gen Word64
arbBlockAddr (BDGeom cnt _sz) =
  fromIntegral `fmap` choose (0 :: Integer, ub)
    where ub = fromIntegral $ cnt - 1

arbBlockData :: BDGeom -> Gen ByteString
arbBlockData (BDGeom _cnt sz) =
  BS.pack `fmap` replicateM (fromIntegral sz) byte

byte :: Gen Word8
byte = fromIntegral `fmap` choose (0 :: Int, 255)
