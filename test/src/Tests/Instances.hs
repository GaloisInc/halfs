{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE FlexibleInstances #-}

module Tests.Instances
where

import Control.Applicative       ((<$>), (<*>))
import Control.Exception         (assert)
import Control.Monad             (foldM, replicateM)
import Data.Bits                 (shiftL)
import Data.ByteString           (ByteString)
import qualified Data.ByteString as BS
import Data.List (nub, genericTake)
import Data.Serialize
import Data.Time
import Data.Word
import System.Random
import Test.QuickCheck
import Test.QuickCheck.Monadic hiding (assert)

import Halfs.BlockMap            (Extent(..))

import Halfs.Inode               ( Cont(..)
                                 , ContRef(..)
                                 , Inode(..)
                                 , InodeRef(..)
                                 , computeNumAddrs
                                 , minimalContSize
                                 , minimalInodeSize
                                 , minContBlocks
                                 , minInodeBlocks
                                 , nilContRef
                                 )
import Halfs.Directory
import Halfs.Protection          (UserID(..), GroupID(..))
import Halfs.SuperBlock          (SuperBlock(..))
import Tests.Types


--------------------------------------------------------------------------------
-- Block Device generators and helpers

-- | Generate an arbitrary geometry (modified by function given by the
-- first parameter) and employ the given geometry-requiring generator to
-- supply data to the given property.
forAllBlocksM :: Monad m =>
                 (BDGeom -> BDGeom)
              -- ^ geometry transformer
              -> (BDGeom -> Gen [(Word64, ByteString)])
              -- ^ data generator
              -> ([(Word64, ByteString)] -> BDGeom -> PropertyM m b)
              -- ^ property constructor   
              -> PropertyM m b
forAllBlocksM f gen prop =
  forAllM arbBDGeom $ \g -> let g' = f g in forAllM (gen g') (flip prop g')

-- | Generate a power of 2 given an exponent range in [0, 63]
powTwo :: Int -> Int -> Gen Word64
powTwo l h = assert (l >= 0 && h <= 63) $ shiftL 1 <$> choose (l, h)

arbBDGeom :: Gen BDGeom
arbBDGeom = arbitrary

-- | Creates arbitrary "filesystem data" : just block addresses and
-- block data, constrained by the given device geometry
arbFSData :: BDGeom -> Gen [(Word64, ByteString)]
arbFSData g = listOf1 $ (,) <$> arbBlockAddr g <*> arbBlockData g

-- | Creates arbitrary contiguous "filesystem data" : consecutive runs
-- of block addresses and accompanying block data, constrained by the
-- given device geometry
arbContiguousData :: BDGeom -> Gen [(Word64, ByteString)]
arbContiguousData g = do
  let numBlks = fromIntegral $ bdgSecCnt g
      limit   = 64 -- artificial limit on number of contiguous blocks
                   -- generated; keeps generated data on the small side
  numContig <- if numBlks > 1 -- maybe we can't create continguous blocks
               then choose (2 :: Integer, max 2 (min limit (numBlks `div` 2)))
               else return 1
  baseAddrs <- (\x -> map fromIntegral [x .. x + numContig])
               `fmap` choose (0 :: Integer, numBlks - numContig)
  zip baseAddrs `fmap` vectorOf (fromIntegral numContig) (arbBlockData g)

arbBlockAddr :: BDGeom -> Gen Word64
arbBlockAddr (BDGeom cnt _sz) =
  fromIntegral `fmap` choose (0 :: Integer, ub)
    where ub = fromIntegral $ cnt - 1

arbBlockData :: BDGeom -> Gen ByteString
arbBlockData (BDGeom _cnt sz) = bytes (fromIntegral sz)

byte :: Gen Word8
byte = fromIntegral `fmap` choose (0 :: Int, 255)

bytes :: Int -> Gen ByteString
bytes n = BS.pack `fmap` replicateM n byte

printableBytes :: Int -> Gen ByteString
printableBytes n = BS.pack `fmap` replicateM n printableByte

printableByte :: Gen Word8
printableByte = fromIntegral `fmap` choose (33 :: Int, 126)

filename :: Int -> Gen String
filename maxLen =
  resize maxLen $
    listOf1 (elements $ ['0' .. '9'] ++ ['A'..'Z'] ++ ['a'..'z'] ++ ['_', '-'])



--------------------------------------------------------------------------------
-- BlockMap generators and helpers

newtype UnallocDecision = UnallocDecision Bool deriving Show

-- | Given an extent, generates subextents that cover it; useful for creating
-- allocation sequences.  To keep the number of subextents relatively small, the
-- first two subextents generated cover ~75% of the input extent.  The input
-- extent should be of reasonable size (>= 8) in order to avoid degenerate
-- corner cases.
arbExtents :: Extent -> Gen [Extent]
arbExtents ext@(Extent _ ub)
  | ub < 8   = fail $ "Tests.Instances.arbExtents: "
               ++ "Input extent size is too small (<8)"
  | otherwise = arbExtents' ext

arbExtents' :: Extent -> Gen [Extent]
arbExtents' (Extent b ub) = do
  filledQuarter <- fill (Extent (b + soFar) (ub - soFar))
  let r = Extent b halfCnt : (Extent (b + halfCnt) quarterCnt : filledQuarter)
  -- monotonically decreasing size over region groups
  assert (halfCnt >= quarterCnt &&
          quarterCnt >= sum (map extSz filledQuarter)) $ do
  -- distinct base addrs
  assert (let bs = map extBase r in length bs == length (nub bs)) $ do
  -- exactly covers input extent and contains and no 0-size extents
  assert (ub == foldr (\e -> assert (extSz e > 0) (extSz e +)) 0 r) $ do
  return r
  where
    halfCnt    = ub `div` 2
    quarterCnt = ub `div` 4 + 1
    soFar      = halfCnt + quarterCnt
    --
    fill :: Extent -> Gen [Extent]
    fill (Extent _ 0)    = return []
    fill (Extent b' ub') = do
      sz   <- choose (1, ub')
      rest <- fill $ Extent (b' + sz) (ub' - sz)
      return (Extent b' sz : rest)

-- | A hacky, inefficient-but-good-enough permuter
permute :: [a] -> Gen [a]
permute xs = do
  let len = fromIntegral $ length xs
  foldM rswap xs $ replicate len len
  where
    rswap [] _       = return []
    rswap (x:xs') len = do
      i <- choose (0, len - 1)
      return $ let (l,r) = splitAt i xs'
               in l ++ [x] ++ r


--------------------------------------------------------------------------------
-- Instances and helpers

instance Arbitrary UnallocDecision where
  arbitrary = UnallocDecision `fmap` arbitrary

-- instance Arbitrary BDGeom where
--   arbitrary = 
--     BDGeom
--     <$> powTwo 10 13   -- 1024..8192 sectors
--     <*> powTwo  9 12   -- 512b..4K sector size
--                        -- => 512K .. 32M filesystem size

instance Arbitrary BDGeom where
  arbitrary = return $ BDGeom 512 512

-- Generate an arbitrary version 1 superblock with coherent free and
-- used block counts.  Block size and count are constrained by the
-- Arbitrary instance for BDGeom.
instance Arbitrary SuperBlock where
  arbitrary = do
    BDGeom cnt sz <- arbitrary
    free          <- choose (0, cnt)
    SuperBlock
      <$> return 1             -- version                         
      <*> return sz            -- blockSize     
      <*> return cnt           -- blockCount    
      <*> arbitrary            -- unmountClean  
      <*> return free          -- freeBlocks    
      <*> return (cnt - free)  -- usedBlocks    
      <*> arbitrary            -- fileCount     
      <*> IR `fmap` arbitrary  -- rootDir                     
      <*> IR `fmap` return 1   -- blockMapStart

-- Generate an arbitrary inode with mostly coherent fields (filesize/allocated
-- block relationships are not cogent, however) based on the minimal inode size
-- computation for an arbitrary device geometry.
instance (Arbitrary a, Ord a, Serialize a, Show a) => Arbitrary (Inode a) where
  arbitrary = do
    BDGeom _ blkSz <- arbitrary
    createTm       <- arbitrary
    addrCnt        <- computeNumAddrs blkSz minInodeBlocks
                        =<< minimalInodeSize createTm
    Inode
      <$> IR `fmap` arbitrary                -- inoParent
      <*> IR `fmap` arbitrary                -- inoAddress
      <*> arbitrary                          -- inoFileSize
      <*> arbitrary                          -- inoAllocBlocks
      <*> arbitrary                          -- inoFileType
      <*> arbitrary                          -- inoMode
      <*> arbitrary                          -- inoNumLinks
      <*> return createTm                    -- inoCreateTime
      <*> arbitrary `suchThat` (>= createTm) -- inoModifyTime
      <*> arbitrary `suchThat` (>= createTm) -- inoAccessTime
      <*> arbitrary `suchThat` (>= createTm) -- inoChangeTime
      <*> UID `fmap` arbitrary               -- inoUser
      <*> GID `fmap` arbitrary               -- inoGroup
      <*> (arbitrary >>= \cont -> do         -- inoCont
             let blockCount' = min (blockCount cont) addrCnt
             return
               cont{ address    = nilContRef
                   , blockCount = blockCount'
                   , blockAddrs = genericTake blockCount' (blockAddrs cont)
                   , numAddrs   = min (numAddrs cont) addrCnt
                   }
          )

                                             
-- Generate an arbitrary inode 'continuation' (a Cont) with coherent
-- fields based on the minimal cont size computaton for an arbitrary
-- device geometry
instance Arbitrary Cont where
  arbitrary = do
    BDGeom _ blkSz <- arbitrary
    addrCnt        <- computeNumAddrs blkSz minContBlocks =<< minimalContSize 
    numBlocks      <- fromIntegral `fmap` choose (0, addrCnt)
    Cont
      <$> CR `fmap` arbitrary                        -- address
      <*> CR `fmap` arbitrary                        -- continuation
      <*> return (fromIntegral numBlocks)            -- blockCount
      <*> replicateM numBlocks arbitrary             -- blockAddrs
      -- Transient:
      <*> return addrCnt                             -- numAddrs

-- Generate an arbitrary directory entry
instance Arbitrary DirectoryEntry where
  arbitrary = 
    DirEnt
      <$> (listOf1 arbitrary :: Gen String)          -- deName
      <*> IR  `fmap` arbitrary                       -- deInode
      <*> UID `fmap` arbitrary                       -- deUser
      <*> GID `fmap` arbitrary                       -- deGroup
      <*> (arbitrary :: Gen FileMode)                -- deMode
      <*> elements [RegularFile, Directory, Symlink] -- deType
    
instance Arbitrary FileMode where
  arbitrary = 
    FileMode <$> gp <*> gp <*> gp
    where
      gp          = validAccess >>= permute
      validAccess = elements [ []
                             , [Read]
                             , [Write]
                             , [Execute]
                             , [Read,Write]
                             , [Read,Execute]
                             , [Write,Execute]
                             , [Read,Write,Execute]
                             ]

instance Arbitrary FileType where
  arbitrary = elements [ RegularFile, Directory, Symlink ]

instance Arbitrary UTCTime where
  arbitrary = UTCTime <$> arbitrary <*> arbitrary

instance Arbitrary Day where
  arbitrary =
    fromGregorian <$> choose (1900, 2200) <*> choose (1, 12) <*> choose (1, 31)

instance Arbitrary DiffTime where
  arbitrary = do
    sec <- choose (0, 86400)
    ps  <- choose (0, 10000000000000) -- 10^13, intentionally exceeds picosecond
                                      -- resolution
    return $ secondsToDiffTime sec + picosecondsToDiffTime ps

instance Arbitrary Word64 where
  arbitrary = choose (0, maxBound)

instance Random Word64 where
  randomR = integralRandomR
  random  = randomR (minBound, maxBound)

integralRandomR :: (Integral a, RandomGen g) => (a,a) -> g -> (a,g)
integralRandomR (a,b) g =
  case randomR (fromIntegral a :: Integer, fromIntegral b :: Integer) g of
    (x, g') -> (fromIntegral x, g')
