{-# LANGUAGE MultiParamTypeClasses, FlexibleContexts, FlexibleInstances #-}
module Halfs.BlockMap(
         BlockMap
       , blockMapSizeBytes
       , blockMapSizeBlocks
       , readBlockMap
       , writeBlockMap
       , markBlocksUsed
       , markBlocksUnused
       , numFreeBlocks
       , getFreeBlocks
       )
 where

import Control.Monad
import Data.Array.MArray
import Data.Bits
import qualified Data.ByteString as BS
import Data.FingerTree
import Data.Monoid
import Data.Word

import Halfs.Classes
import System.Device.BlockDevice

-- ----------------------------------------------------------------------------

data Extent = Extent { baseBlock :: Word64, size :: Word64 }

newtype ExtentSize = ES Word64

instance Monoid ExtentSize where
  mempty                = ES minBound
  mappend (ES a) (ES b) = ES (max a b)

instance Measured ExtentSize Extent where
  measure (Extent _ size) = ES size

findBlock :: Word64 -> ExtentSize -> Bool
findBlock x (ES y) = y > x

insert :: Extent -> FingerTree ExtentSize Extent ->
          FingerTree ExtentSize Extent
insert a@(Extent _ size) tr = treeL >< (a <| treeR)
 where (treeL, treeR) = split (findBlock size) tr

-- ----------------------------------------------------------------------------

data BlockMap a r = BM {
    freeTree :: r (FingerTree ExtentSize Extent)
  , freeMap  :: a Word64 Bool -- ^Is the given block free?
  , numFree  :: r Word64
  }

-- |How many bytes of space are needed to store a block map for the given
-- number of blocks?
blockMapSizeBytes :: Word64 -> Word64
blockMapSizeBytes x = (x + 7) `div` 8

-- |How many blocks of space are needed to store a block map for the given
-- number of blocks?
blockMapSizeBlocks :: Word64 -> Word64
blockMapSizeBlocks x = (bytes + (x - 1)) `div` x
 where bytes = blockMapSizeBytes x

-- |Read in the block map from the disk
readBlockMap :: (Monad m, Reffable r m, MArray a Bool m) =>
                BlockDevice m ->
                m (BlockMap a r)
readBlockMap dev = do
  -- unsafeInterleaveIO is what we want, here, but this is probably the
  -- only place where its use is safe, so we do this by hand.
  bArr <- newArray (0, numBlocks - 1) False
  (numFree, _, freeMap) <- foldM (pullIn bArr) (0, Nothing, empty) bmapBlocks
  tree <- newRef freeMap
  free <- newRef numFree
  return $ BM tree bArr free
 where
  numBlocks      = bdNumBlocks dev
  blockSize      = bdBlockSize dev
  bmapSize       = blockMapSizeBlocks numBlocks
  bmapBlocks     = [1..numBlocks]
  blocksPerBlock = blockSize * 8
  --
  pullIn arr acc bNum = do
    block <- bdReadBlock dev bNum
    let bytes     = BS.unpack block
        baseOff   = bNum * blocksPerBlock
        blockOffs = map (+ baseOff) $ map (* 8) [0..blockSize-1]
    foldM (pullIn' arr) acc (zip blockOffs bytes)
  pullIn' arr acc (baseOff, datum) = do
    let blocks    = map (+ baseOff) [0..7]
        bits      = map (testBit datum) [0..7]
    foldM (pullIn'' arr) acc (zip blocks bits)
  --
  pullIn'' arr res@(freeBs, Nothing, map) (block, False) =
    return res
  pullIn'' arr res@(freeBs, Nothing, map) (block, True)  = do
    writeArray arr block True
    return (freeBs + 1, Just block, map)
  pullIn'' arr res@(freeBs, Just f,  map) (block, False) =
    return (freeBs, Nothing, insert (Extent f (block - f)) map)
  pullIn'' arr res@(freeBs, Just f,  map) (block, True)  = do
    writeArray arr block True
    return (freeBs + 1, Just f, map)

-- |Write the block map to the disk
writeBlockMap :: (Monad m, Reffable r m, MArray a Bool m) =>
                 BlockDevice m -> BlockMap a r ->
                 m ()
writeBlockMap dev bmap = do
  bits <- getElems (freeMap bmap)
  let bits'     = byN 8 bits
      bytes     = map bitsToByte bits'
      bsBlocks  = map BS.pack $ byN blockSize bytes
      writeCmds = zip bsBlocks [1..length bsBlocks]
  forM_ writeCmds $ \ (block, off) ->
    bdWriteBlock dev (fromIntegral off) block
 where
  blockSize = fromIntegral $ bdBlockSize dev
  --
  byN :: Int -> [a] -> [[a]]
  byN _ [] = []
  byN n xs = let (firstN, rest) = splitAt n xs
             in firstN : (byN n rest)
  bitsToByte :: [Bool] -> Word8
  bitsToByte xs = foldr (\ cur acc -> (acc `shiftL` 1) + cur) 0 $
                    map (\ x -> if x then 1 else 0) xs

-- |Mark a given set of blocks as used
markBlocksUsed :: (Monad m, Reffable r m, MArray a Bool m) =>
                  BlockMap a r -> Word64 -> Word64 ->
                  m ()
markBlocksUsed = undefined

-- |Mark a given set of blocks as unused
markBlocksUnused :: (Monad m, Reffable r m, MArray a Bool m) =>
                    BlockMap a r -> Word64 -> Word64 ->
                    m ()
markBlocksUnused = undefined

-- |Return the number of blocks currently left
numFreeBlocks :: (Monad m, Reffable r m, MArray a Bool m) =>
                 BlockMap a r ->
                 m Word64
numFreeBlocks bm = readRef (numFree bm)

-- |Get a set of blocks from the disk. This routine will attempt to fetch
-- a contiguous set of blocks, but isn't guaranteed to do so. If there aren't
-- enough blocks, returns Nothing.
getFreeBlocks :: (Monad m, Reffable r m, MArray a Bool m) =>
                 BlockMap a r -> Word64 ->
                 m (Maybe [Word64])
getFreeBlocks = undefined
