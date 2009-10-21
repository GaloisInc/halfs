{-# LANGUAGE MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, BangPatterns #-}
module Halfs.BlockMap(
         BlockMap
       , newBlockMap
       , readBlockMap
       , writeBlockMap
       , markBlocksUnused
       , numFreeBlocks
       , getBlocks
       )
 where

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

-- ----------------------------------------------------------------------------
--
-- Important block format diagram
--
--                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3 3 3 3 3
--  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
-- +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
-- |S|M| | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | | |
-- +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
--
--

data Extent = Extent { baseBlock :: Word64, size :: Word64 }

newtype ExtentSize = ES Word64

instance Monoid ExtentSize where
  mempty                = ES minBound
  mappend (ES a) (ES b) = ES (max a b)

instance Measured ExtentSize Extent where
  measure (Extent _ s)  = ES s

findBlock :: Word64 -> ExtentSize -> Bool
findBlock x (ES y) = y > x

insert :: Extent -> FingerTree ExtentSize Extent ->
          FingerTree ExtentSize Extent
insert a@(Extent _ s) tr = treeL >< (a <| treeR)
 where (treeL, treeR) = split (findBlock s) tr

-- ----------------------------------------------------------------------------

data BlockMap b r = BM {
    freeTree :: r (FingerTree ExtentSize Extent)
  , usedMap  :: b -- ^Is the given block free?
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

-- |Create a new block map that will hold the given number of entries.
newBlockMap :: (Monad m, Reffable r m, Bitmapped b m) =>
               Word64 ->
               m (BlockMap b r)
newBlockMap numBlocks = do
  bArr <- newBitmap totalBlocks False
  tree <- newRef initialTree
  free <- newRef freeBlocks
  setBit bArr 0
  forM_ [1..mapBlockSize] $ setBit bArr
  forM_ [numBlocks..totalBlocks] $ setBit bArr
  return $ BM tree bArr free
 where
  sizeb        = blockMapSizeBytes numBlocks
  totalBlocks  = sizeb * 8
  mapBlockSize = blockMapSizeBlocks totalBlocks
  freeBlocks   = numBlocks - mapBlockSize
  initialTree  = singleton $ Extent {
                   baseBlock = mapBlockSize + 1
                 , size      = freeBlocks
                 }

-- |Read in the block map from the disk
readBlockMap :: (Monad m, Reffable r m, Bitmapped b m) =>
                BlockDevice m ->
                m (BlockMap b r)
readBlockMap dev = do
  -- unsafeInterleaveIO is what we want, here, but this is probably the
  -- only place where its use is safe, so we do this by hand.
  bArr <- newBitmap numBlocks False
  free <- newRef 0
  forM_ [1..blockMapBlocks] $ \ block -> do
    let baseAddr = (block - 1) * 8 * bdBlockSize dev
    blockBS <- bdReadBlock dev block
    forM_ [0..bdBlockSize dev - 1] $ \ byteIndex -> do
      let byte            = BS.index blockBS (fromIntegral byteIndex)
      forM_ [0..7] $ \ offset -> do
        if (testBit byte offset)
          then setBit bArr (baseAddr + byteIndex + fromIntegral offset)
          else do cur <- readRef free
                  writeRef free $! cur + 1
  baseTree <- newRef empty
  getFreeBlocks bArr baseTree Nothing 0
  return $ BM baseTree bArr free
 where
  numBlocks      = bdNumBlocks dev
  totalBlocks    = blockMapSizeBytes numBlocks * 8
  blockMapBlocks = blockMapSizeBlocks totalBlocks
  --
  getFreeBlocks    _       _ Nothing   cur | cur == totalBlocks =
    return ()
  getFreeBlocks    _ treeRef (Just s)  cur | cur == totalBlocks = do
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

-- |Write the block map to the disk
writeBlockMap :: (Monad m, Reffable r m, Bitmapped b m, Functor m) =>
                 BlockDevice m -> BlockMap b r ->
                 m ()
writeBlockMap dev bmap =
  forM_ [0..bitBlocks - 1] $ \ base -> do
    block <- offsetToBlock (usedMap bmap) (base * bitsPerBlock)
    bdWriteBlock dev (base + 1) block
 where
  bitsPerBlock = bdBlockSize dev * 8
  bitBlocks    = blockMapSizeBlocks (bdNumBlocks dev)
  --
  offsetToBlock bArr index =
    BS.pack `fmap` (forM [index .. index+bitsPerBlock] (offsetToByte bArr))
  offsetToByte bArr index = do
    b0 <- checkBit bArr (index + 0)
    b1 <- checkBit bArr (index + 1)
    b2 <- checkBit bArr (index + 2)
    b3 <- checkBit bArr (index + 3)
    b4 <- checkBit bArr (index + 4)
    b5 <- checkBit bArr (index + 5)
    b6 <- checkBit bArr (index + 6)
    b7 <- checkBit bArr (index + 7)
    let !r0 = if b0 then 1             else 0
        !r1 = if b1 then B.setBit r0 1 else r0
        !r2 = if b2 then B.setBit r1 1 else r1
        !r3 = if b3 then B.setBit r2 1 else r2
        !r4 = if b4 then B.setBit r3 1 else r3
        !r5 = if b5 then B.setBit r4 1 else r4
        !r6 = if b6 then B.setBit r5 1 else r5
        !r7 = if b7 then B.setBit r6 1 else r6
    return r7

-- |Mark a given set of blocks as unused
markBlocksUnused :: (Monad m, Reffable r m, Bitmapped b m) =>
                    BlockMap b r -> Word64 -> Word64 ->
                    m ()
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
