{-# LANGUAGE MultiParamTypeClasses, FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances, BangPatterns #-}
module Halfs.BlockMap(
         BlockMap(..)
       , newBlockMap
       , readBlockMap
       , writeBlockMap
       , allocBlocks
       , unallocBlocks
       , unallocBlocksContig
       , numFreeBlocks
       )
 where

import Control.Exception (assert)
import Control.Monad
import Data.Bits hiding (setBit)
import qualified Data.Bits as B
import qualified Data.ByteString as BS
import Data.FingerTree
import qualified Data.Foldable as DF
import Data.List (sort)
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

{-

TODO: 

 * Fix TODOs scattered around regarding usedMap modifications (not)
   escaping alloc/unalloc functions

 * The distinction between unallocBlocksContig and unallocBlocks is
   gross, as is the mixed representation between start/end and groups of
   blocks.  Since allocations are coming from the free tree anyway, we
   might as well just use Extents, and have the allocator yield
   something like (e.g.):

   data BlockGroup = Contig Extent | Discontig [Extent]

   together with a simple toplevel function to expand an extent into
   block addresses.

   This'll make life a lot easier when we want to unalloc block groups.

-}

data Extent = Extent { _extBase :: Word64, _extSz :: Word64 }
  deriving (Show, Eq)

newtype ExtentSize = ES Word64
type    FreeTree   = FingerTree ExtentSize Extent

instance Monoid ExtentSize where
  mempty                = ES minBound
  mappend (ES a) (ES b) = ES (max a b)

instance Measured ExtentSize Extent where
  measure (Extent _ s)  = ES s

splitBlockSz :: Word64 -> FreeTree -> (FreeTree, FreeTree)
splitBlockSz sz = split $ \(ES y) -> y >= sz

insert :: Extent -> FreeTree -> FreeTree
insert a@(Extent _ sz) tr = treeL >< (a <| treeR)
 where (treeL, treeR) = splitBlockSz sz tr

-- ----------------------------------------------------------------------------

data BlockMap b r = BM {
    bmFreeTree :: r FreeTree
  , bmUsedMap  :: b -- ^ Is the given block free?
  , bmNumFree  :: r Word64
  }

-- | Calculate the number of bytes required to store a block map for the
-- given number of blocks
blockMapSizeBytes :: Word64 -> Word64
blockMapSizeBytes numBlks = numBlks `divCeil` 8

-- | Calculate the number of blocks required to store a block map for
-- the given number of blocks.
blockMapSizeBlks :: Word64 -> Word64 -> Word64
blockMapSizeBlks numBlks blkSzBytes = bytes `divCeil` blkSzBytes
  where bytes = blockMapSizeBytes numBlks

-- | Create a new block map for the given device geometry
newBlockMap :: (Monad m, Reffable r m, Bitmapped b m) =>
               BlockDevice m
            -> m (BlockMap b r)
newBlockMap dev = do
  when (numBlks < 3) $ fail "Block device is too small for block map creation"

  trace ("newBlockMap: numBlks = " ++ show numBlks) $ do
  trace ("newBlockMap: totalBits = " ++ show totalBits) $ do
  trace ("newBlockMap: blockMapSzBlks = " ++ show blockMapSzBlks) $ do
  trace ("newBlockMap: baseFreeIdx = " ++ show baseFreeIdx) $ do
  trace ("newBlockMap: numFree = " ++ show numFree) $ do

  -- We overallocate the bitmap up to the entire size of the block(s)
  -- needed for the block map region so that de/serialization in the
  -- {read,write}BlockMap functions is straightforward
  bArr <- newBitmap totalBits False
  let markUsed (l,h) = forM_ [l..h] (setBit bArr)
  mapM_ markUsed
    [ (0, 0)                   -- superblock
    , (1, blockMapSzBlks)      -- blocks for storing the block map
    , (numBlks, totalBits - 1) -- overallocated region
    ] 
  treeR    <- newRef $ singleton $ Extent baseFreeIdx numFree
  numFreeR <- newRef numFree
  return $ assert (baseFreeIdx + numFree == numBlks) $
    BM treeR bArr numFreeR
 where
  numBlks        = bdNumBlocks dev
  totalBits      = blockMapSzBlks * bdBlockSize dev * 8
  blockMapSzBlks = blockMapSizeBlks numBlks (bdBlockSize dev)
  baseFreeIdx    = blockMapSzBlks + 1
  numFree        = numBlks - blockMapSzBlks - 1 {- -1 for superblock -}

-- | Read in the block map from the disk
readBlockMap :: (Monad m, Reffable r m, Bitmapped b m) =>
                BlockDevice m
             -> m (BlockMap b r)
readBlockMap dev = do
  bArr  <- newBitmap totalBits False
  freeR <- newRef 0

  -- Unpack the block map's block region into the empty bitmap
  forM_ [0..blockMapSzBlks - 1] $ \blkIdx -> do
    blockBS <- bdReadBlock dev (blkIdx + 1 {- +1 for superblock -})
    forM_ [0..bdBlockSize dev - 1] $ \byteIdx -> do
      let byte = BS.index blockBS (fromIntegral byteIdx)
      forM_ [0..7] $ \bitIdx -> do
        if (testBit byte bitIdx)
         then do let baseByte = blkIdx * bdBlockSize dev
                     idx      = (baseByte + byteIdx) * 8 + fromIntegral bitIdx 
                 setBit bArr idx
         else do cur <- readRef freeR
                 writeRef freeR $! cur + 1
  
  baseTreeR <- newRef empty
  getFreeBlocks bArr baseTreeR Nothing 0
  return $ BM baseTreeR bArr freeR
 where
  numBlks        = bdNumBlocks dev
  totalBits      = blockMapSzBlks * bdBlockSize dev * 8
  blockMapSzBlks = blockMapSizeBlks numBlks (bdBlockSize dev)
  -- 
  writeExtent treeR ext@(Extent _b _s) = do
--    trace ("writing extent " ++ show (b,s)) $ do
    t <- readRef treeR
    writeRef treeR $! insert ext t
  -- 
  -- getFreeBlocks recurses over each used bit in the used bitmap and
  -- finds runs of free block regions, inserting representative Extents
  -- into the "free tree" as it does so.  The third parameter tracks the
  -- block address of start of the current free region.
  getFreeBlocks _bmap treeR mbase cur | cur == totalBits =
    maybe (return ())
          (\base -> writeExtent treeR (Extent base $ cur - base))
          mbase

  getFreeBlocks bmap treeR Nothing cur = do
    used <- checkBit bmap cur
--    trace ("HERE 1: cur = " ++ show cur ++ ", used = " ++ show used) $ do
    getFreeBlocks bmap treeR (if used then Nothing else Just cur) (cur + 1)

  getFreeBlocks bmap treeR b@(Just base) cur = do
    used <- checkBit bmap cur
--    trace ("HERE 2: cur = " ++ show cur ++ ", used = " ++ show used) $ do
    when used $ writeExtent treeR (Extent base $ cur - base)
    getFreeBlocks bmap treeR (if used then Nothing else b) (cur + 1)

-- | Write the block map to the disk
writeBlockMap :: (Monad m, Reffable r m, Bitmapped b m, Functor m) =>
                 BlockDevice m
              -> BlockMap b r
              -> m ()
writeBlockMap dev bmap = do
  -- Pack the used bitmap into the block map's block region
  forM_ [0..blockMapSzBlks - 1] $ \blkIdx -> do
    blockBS <- BS.pack `fmap` forM [0..bdBlockSize dev - 1] (getBytes blkIdx)
    bdWriteBlock dev (blkIdx + 1 {- +1 for superblock -}) blockBS
  where
    numBlks        = bdNumBlocks dev
    blockMapSzBlks = blockMapSizeBlks numBlks (bdBlockSize dev)
    --
    getBytes blkIdx byteIdx = do
      bs <- forM [0..7] $ \bitIdx -> do
        let base = blkIdx * bdBlockSize dev
            idx  = (base + byteIdx) * 8 + bitIdx
        checkBit (bmUsedMap bmap) idx
      return $ foldr (\(b,i) r -> if b then B.setBit r i else r)
               (0::Word8) (bs `zip` [0..7])
            
-- | Allocate a set of blocks from the disk. This routine will attempt
-- to fetch a contiguous set of blocks, but isn't guaranteed to do so.
-- Contiguous blocks are represented via the Contig constructor of the
-- BlockGroup datatype; discontiguous blocks via Discontig.  If there
-- aren't enough blocks available, this function yields Nothing.
allocBlocks :: (Monad m, Reffable r m, Bitmapped b m) =>
               BlockMap b r
            -- ^ the block map 
            -> Word64
            -- ^ requested number of blocks to allocate
            -> m (Maybe [Word64])
allocBlocks bm numBlocks = do
  available <- readRef $! bmNumFree bm
  if available < numBlocks
    then return Nothing
    else do freeTree <- readRef $! bmFreeTree bm
            let (blocks, freeTree') = findSpace numBlocks freeTree
            forM_ blocks $ setBit (bmUsedMap bm)
            writeRef (bmFreeTree bm) freeTree'
            writeRef (bmNumFree bm) (available - numBlocks)
            return (Just blocks)

findSpace :: Word64 -> FreeTree -> ([Word64], FreeTree)
findSpace goalSz freeTree =
  -- Precondition: There is sufficient space in the free tree to accomodate the
  -- given goal size, although that space may not be contiguous
  assert preCond $
  case viewl treeR of
    EmptyL -> 
      -- Cannot find an extent large enough, so gather smaller extents
      trace ("rngs = " ++ show rngs ++ "\n, treeL' = " ++ show treeL') $
      (concat rngs, treeL') where (rngs, treeL') = gatherL (viewr treeL) 0 []

    Extent b sz :< treeR' -> 
      -- Found an extent with size >= the goal size
      ( blockRange b goalSz
      , treeL
        ><
        -- Split the extent when it exceeds the goal size
        if sz > goalSz
        then singleton $ Extent (b + goalSz) (sz - goalSz)
        else empty
        ><
        treeR'
      )
  where
    preCond         = goalSz <= DF.foldr (\(Extent _ s1) -> (s1 +)) 0 freeTree
    (treeL, treeR)  = splitBlockSz goalSz freeTree
    blockRange b sz = [b .. b + sz - 1]
    -- 
    gatherL :: ViewR (FingerTree ExtentSize) Extent
            -> Word64
            -> [[Word64]]
            -> ([[Word64]], FreeTree)
    gatherL EmptyR _ _ = error "Precondition violated: insufficent space"
    gatherL (treeL' :> Extent b sz) accSz accRngs
      | accSz + sz < goalSz = gatherL (viewr treeL')
                                      (accSz + sz)
                                      (blockRange b sz : accRngs) 
      | accSz + sz == goalSz = (blockRange b sz : accRngs, treeL')
      | otherwise =
          -- We've exceeded the goal, so split the extent we just encountered
          (blockRange (b + diff) (sz - diff) : accRngs, treeL' >< extra)
          where diff  = accSz + sz - goalSz
                extra = singleton $ Extent b diff

-- | Mark a given set of contiguous blocks as unused
unallocBlocksContig :: (Monad m, Reffable r m, Bitmapped b m) =>
                       BlockMap b r -- ^ the block map
                    -> Word64       -- ^ start block address
                    -> Word64       -- ^ end block address
                    -> m ()
unallocBlocksContig bm s e = do
  -- TODO/FIXME: modify usedMap here 
  assert (e >= s) $ do
  available <- readRef $! bmNumFree bm
  freeTree  <- readRef $! bmFreeTree bm
  writeRef (bmFreeTree bm) $ insert (Extent s numBlocks) freeTree
  writeRef (bmNumFree bm)  $ available + numBlocks
  where
    numBlocks = e - s + 1

-- | Unallocate the given set of blocks; note that if the set of blocks
-- is known to be contiguous, unallocBlocksContig is preferred
unallocBlocks :: (Monad m, Reffable r m, Bitmapped b m) =>
                 BlockMap b r -- ^ the block map
              -> [Word64]     -- ^ the blocks to unalloc
              -> m ()
unallocBlocks _ []  = return ()
unallocBlocks bm bs = mapM_ (uncurry $ unallocBlocksContig bm) (contigExts bs)
  where
    contigExts     = map toExt . contigGroups . sort
    toExt []       = error "Empty contiguous group: should not happen"
    toExt xs@(x:_) = (x, x + fromIntegral (length xs) - 1)

-- |Return the number of blocks currently left
numFreeBlocks
  :: (Monad m, Reffable r m, Bitmapped b m) =>
     BlockMap b r
  -> m Word64
numFreeBlocks = readRef . bmNumFree

--------------------------------------------------------------------------------
-- Utility functions

divCeil :: Integral a => a -> a -> a
divCeil a b = (a + (b - 1)) `div` b

-- Group by, but compares adjacent elements in the input list
adjGroupBy :: (a -> a -> Bool) -> [a] -> [[a]]
adjGroupBy _ []     =  []
adjGroupBy p (x:xs) = (x:ys) : adjGroupBy p zs
  where (ys,zs) = aux x xs
        --
        aux x' (q:qs) | p x' q = (q:ys', zs') where (ys',zs') = aux q qs
        aux _ xs'              = ([], xs')

contigGroups :: Num a => [a] -> [[a]]
contigGroups = adjGroupBy $ \x y -> y - x == 1

