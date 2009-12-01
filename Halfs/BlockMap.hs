{-# LANGUAGE MultiParamTypeClasses, FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances, BangPatterns #-}
module Halfs.BlockMap(
         BlockMap(..)
       , newBlockMap
       , readBlockMap
       , writeBlockMap
       , markBlocksUnused
       , numFreeBlocks
       , getBlocks
       )
 where

import Control.Exception (assert)
import Control.Monad
import Data.Bits hiding (setBit)
import qualified Data.Bits as B
import qualified Data.ByteString as BS
import Data.FingerTree
import Data.Monoid
import Data.Word
import Prelude hiding (null)

import Halfs.Classes
import System.Device.BlockDevice

-- temp
import Debug.Trace
-- temp  

-- ----------------------------------------------------------------------------
--
-- Important block format diagram for a ficticious block device w/ 36 blocks;
-- note that the superblock always consumes exactly one block, while the
-- blockmap itself may span multiple blocks as needed, depending on device
-- geometry.
--
--                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3 3 3 3 3
--  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
-- +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
-- |S|M| | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | |
-- +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
--
--

data Extent = Extent { _extBase :: Word64, _extSz :: Word64 } deriving Show

newtype ExtentSize = ES Word64

instance Monoid ExtentSize where
  mempty                = ES minBound
  mappend (ES a) (ES b) = ES (max a b)

instance Measured ExtentSize Extent where
  measure (Extent _ s)  = ES s

findBlock :: Word64 -> ExtentSize -> Bool
findBlock x (ES y) = y > x

insert :: Extent
       -> FingerTree ExtentSize Extent
       -> FingerTree ExtentSize Extent
insert a@(Extent _ s) tr = treeL >< (a <| treeR)
 where (treeL, treeR) = split (findBlock s) tr

-- ----------------------------------------------------------------------------

data BlockMap b r = BM {
    freeTree :: r (FingerTree ExtentSize Extent)
  , usedMap  :: b -- ^ Is the given block free?
  , numFree  :: r Word64
  }

-- | Calculate the number of bytes required to store a block map for the
-- given number of blocks
blockMapSizeBytes :: Word64 -> Word64
blockMapSizeBytes numBlks = numBlks `divRoundUp` 8

-- | Calculate the number of blocks required to store a block map for
-- the given number of blocks.
blockMapSizeBlks :: Word64 -> Word64 -> Word64
blockMapSizeBlks numBlks blkSzBytes = bytes `divRoundUp` blkSzBytes
  where bytes = blockMapSizeBytes numBlks

-- | Create a new block map for the given device geometry
newBlockMap :: (Monad m, Reffable r m, Bitmapped b m) =>
               BlockDevice m
            -> m (BlockMap b r)
newBlockMap dev = do
  when (numBlks < 3) $ fail "Block device is too small for block map creation"

  -- temp
--   trace ("newBlockMap: totalBits = " ++ show totalBits) $ do
--   trace ("newBlockMap: blockMapSzBlks = " ++ show blockMapSzBlks) $ do
--   trace ("newBlockMap: baseFreeIdx = " ++ show baseFreeIdx) $ do
--   trace ("newBlockMap: freeBlocks = " ++ show freeBlocks) $ do                    
  -- temp

  -- We overallocate the bitmap up to the nearest byte boundary for
  -- straightforward de/serialization in the {read,write}BlockMap functions
  bArr <- newBitmap totalBits False
  let markUsed (l,h) = forM_ [l..h] (setBit bArr)
  mapM_ markUsed
    [ (0, 0)                   -- superblock
    , (1, blockMapSzBlks)      -- blocks for storing the block map
    , (numBlks, totalBits - 1) -- overallocated region
    ] 
  tree <- newRef initialTree
  free <- newRef freeBlocks
  return $ assert (baseFreeIdx + freeBlocks == numBlks) $
    BM tree bArr free
 where
  numBlks        = bdNumBlocks dev
  totalBits      = blockMapSzBlks * bdBlockSize dev * 8
  blockMapSzBlks = blockMapSizeBlks numBlks (bdBlockSize dev)
  baseFreeIdx    = blockMapSzBlks + 1
  freeBlocks     = numBlks - blockMapSzBlks - 1 -- NB: superblock is reserved
  initialTree    = singleton $ Extent baseFreeIdx freeBlocks

-- | Read in the block map from the disk
readBlockMap :: (Monad m, Reffable r m, Bitmapped b m) =>
                BlockDevice m ->
                m (BlockMap b r)
readBlockMap dev = do
  bArr <- newBitmap totalBits False
  free <- newRef 0

  forM_ [0..blockMapSzBlks - 1] $ \blkIdx -> do
    blockBS <- bdReadBlock dev (blkIdx + 1 {- superblock -})
    forM_ [0..bdBlockSize dev - 1] $ \byteIdx -> do
      let byte = BS.index blockBS (fromIntegral byteIdx)
      forM_ [0..7] $ \bitIdx -> do
        if (testBit byte bitIdx)
          then do
            let base = blkIdx * bdBlockSize dev
            setBit bArr $ (base + byteIdx) * 8 + fromIntegral bitIdx
          else do cur <- readRef free
                  writeRef free $! cur + 1

  baseTree <- newRef empty
  -- getFreeBlocks bArr baseTree Nothing 0
  return $ BM baseTree bArr free
 where
  numBlks        = bdNumBlocks dev
  totalBits      = blockMapSzBlks * bdBlockSize dev * 8
  blockMapSzBlks = blockMapSizeBlks numBlks (bdBlockSize dev)
  --
{-
  getFreeBlocks    _       _ Nothing   cur | cur == totalBlks =
    return ()
  getFreeBlocks    _ treeRef (Just s)  cur | cur == totalBlks = do
    curTree <- readRef treeRef
    writeRef treeRef $! insert (Extent s $ cur - s) curTree
  getFreeBlocks bmap treeRef Nothing   cur = do
    val <- checkBit bmap cur
    if val
      then getFreeBlocks bmap treeRef Nothing (cur + 1)
      else getFreeBlocks bmap treeRef (Just cur) (cur + 1)
  getFreeBlocks bmap treeRef (Just s) cur  = do
    val <- checkBit bmap cur
    if val
      then do curTree <- readRef treeRef
              writeRef treeRef $! insert (Extent s $ cur - s) curTree
              getFreeBlocks bmap treeRef Nothing (cur + 1)
      else getFreeBlocks bmap treeRef (Just s) (cur + 1)
-}

-- | Write the block map to the disk
writeBlockMap :: (Monad m, Reffable r m, Bitmapped b m, Functor m) =>
                 BlockDevice m
              -> BlockMap b r
              -> m ()
writeBlockMap dev bmap = do
  forM_ [0..blockMapSzBlks - 1] $ \blkIdx -> do
    blockBS <- BS.pack `fmap` forM [0..bdBlockSize dev - 1] (getBytes blkIdx)
--    trace ("writeBlockMap: writing block data = " ++ show blockBS) $ do
    bdWriteBlock dev (blkIdx + 1 {- superblock -}) blockBS
  where
    numBlks        = bdNumBlocks dev
    blockMapSzBlks = blockMapSizeBlks numBlks (bdBlockSize dev)
    --
    getBytes blkIdx byteIdx = do
      bs <- forM [0..7] $ \bitIdx -> do
        let base = blkIdx * bdBlockSize dev
            idx  = (base + byteIdx) * 8 + bitIdx
--        trace ("blkIdx = " ++ show blkIdx ++ ", byteIdx = " ++ show byteIdx
--              ++ ", idx = " ++ show idx) $ do
        checkBit (usedMap bmap) idx
      return $ foldr (\(b,i) r -> if b then B.setBit r i else r)
               (0::Word8) (bs `zip` [0..7])
            
-- |Mark a given set of blocks as unused
markBlocksUnused :: (Monad m, Reffable r m, Bitmapped b m) =>
                    BlockMap b r -- ^ the block map
                 -> Word64       -- ^ start block address
                 -> Word64       -- ^ end block address
                 -> m ()
markBlocksUnused = undefined

-- |Return the number of blocks currently left
numFreeBlocks :: (Monad m, Reffable r m, Bitmapped b m) =>
                 BlockMap b r ->
                 m Word64
numFreeBlocks bm = readRef (numFree bm)

-- |Get a set of blocks from the disk. This routine will attempt to fetch
-- a contiguous set of blocks, but isn't guaranteed to do so. If there aren't
-- enough blocks, returns Nothing.
getBlocks :: (Monad m, Reffable r m, Bitmapped b m) =>
             BlockMap b r -> Word64 ->
             m (Maybe [Word64])
getBlocks bm s = do
  available <- readRef $! numFree bm
  if available < s
    then return Nothing
    else do fMap <- readRef $! freeTree bm
            let (blocks, fMap') = findSpace fMap s
            forM_ blocks $ setBit (usedMap bm)
            writeRef (freeTree bm) fMap'
            writeRef (numFree bm) (available - s)
            return (Just blocks)
 where
  findSpace :: FingerTree ExtentSize Extent -> Word64 ->
               ([Word64], FingerTree ExtentSize Extent)
  findSpace = undefined

--------------------------------------------------------------------------------
-- Utility functions

-- | divRoundUp x k divides x by k, rounding up to next multiple of k
divRoundUp :: Integral a => a -> a -> a
divRoundUp a b = (a + (b - 1)) `div` b