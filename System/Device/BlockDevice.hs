{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances #-}
module System.Device.BlockDevice(
         BlockDevice(..)
       , newCachedBlockDevice
       , newRescaledBlockDevice
       , runBCM
       )
 where

import Control.Monad.State.Strict
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Data.Map(Map)
import qualified Data.Map as Map
import Data.Word

import Debug.Trace

-- |The data type that describes the interface to a BlockDevice. If you can 
-- fill this out reasonably, you can be a file system backend.
data Monad m => BlockDevice m = BlockDevice {
    -- |The size of the smallest read/write block for the device, in bytes.
    bdBlockSize  :: Word64
    -- |The number of blocks in the device.
  , bdNumBlocks  :: Word64
    -- |Read a the given block number for the device, where block numbers run
    -- from 0 to (bdNumBlocks - 1). Smart / paranoid block device implementers
    -- should probably verify their inputs, just in case ...
  , bdReadBlock  :: Word64 -> m ByteString
    -- |Write a given block to the given block number. See the note for
    -- bdReadBlock for indexing and paranoia information.
  , bdWriteBlock :: Word64 -> ByteString -> m ()
    -- |Force any unfinished writes to be written to disk.
  , bdFlush      :: m ()
    -- |Shutdown the device cleanly.
  , bdShutdown   :: m ()
  }

-- ----------------------------------------------------------------------------
--
-- Wrapper that will rescale blocks to a more appropriate size. Note that
-- doing this will require that the desired block size is a multiple of the
-- underlying block size.
--
-- ----------------------------------------------------------------------------

-- |Create a new block device that rescales the block size of the underlying
-- device. This is helpful if, for example, your underlying device has a block
-- size of 512 bytes but you want to deal with it in terms of blocks of 4k.
--
-- Note that this returns a Maybe because it requires that the desired block
-- size be a multiple of the underlying block size. Also note that the
-- resulting block device may be smaller than the original block device, if
--    (bdNumBlocks old `mod` (bdBlockSize new `div` bdBlockSize old)) != 0
newRescaledBlockDevice :: Monad m =>
                          Word64 -> BlockDevice m ->
                          Maybe (BlockDevice m)
newRescaledBlockDevice bsize dev
  | bsize `mod` bdBlockSize dev /= 0 = trace ("bsize=" ++ show bsize ++ ", bdBlockSize dev = " ++ show (bdBlockSize dev)) Nothing
  | otherwise                        = Just res
 where
  res = dev {
    bdBlockSize  = bsize
  , bdNumBlocks  = blocks
  , bdReadBlock  = readBlock
  , bdWriteBlock = writeBlock
  }
  oldbs          = fromIntegral $! bdBlockSize dev
  ratio          = bsize `div` bdBlockSize dev
  blocks         = bdNumBlocks dev `div` ratio
  readBlock  i   = do let start = i * ratio
                          end   = (start + ratio) - 1
                      blks <- forM [start..end] $ bdReadBlock dev
                      return $ BS.concat blks
  writeBlock i b = write i b
  --
  write i b | BS.null b = return ()
            | otherwise = do let (start, rest) = BS.splitAt oldbs b
                             bdWriteBlock dev i start
                             write (i + 1) rest


-- ----------------------------------------------------------------------------
--
-- Wrapper for automatically caching block reads and writes
--
-- ----------------------------------------------------------------------------

type BCM m      = StateT CacheState m
type BlockMap   = Map Word64 (ByteString, Bool, Word64)

data CacheState = BCS {
    _cache     :: BlockMap
  , _timestamp :: Word64
  }

-- |Given an existing block device, creates a new block device that will cache
-- reads and writes.
newCachedBlockDevice :: Monad m => Int -> BlockDevice m -> BlockDevice (BCM m)
newCachedBlockDevice size dev = dev {
    bdReadBlock  = readBlock
  , bdWriteBlock = writeBlock
  , bdFlush      = flush
  , bdShutdown   = lift $ bdShutdown dev
  }
 where
  readBlock  i   = runCacheOp $ \ cache ts ->
                     case Map.lookup i cache of
                       Just (res, dirty ,_) -> do
                         return (Map.insert i (res, dirty, ts) cache, res)
                       Nothing  -> do
                         cache' <- if cacheFull cache
                                     then evictEntry cache dev
                                     else return cache
                         b <- lift $ bdReadBlock dev i
                         return (Map.insert i (b,False,ts) cache', b)
  writeBlock i b = runCacheOp $ \ cache ts ->
                     case Map.lookup i cache of
                       Just _  -> 
                         return (Map.insert i (b, True, ts) cache, ())
                       Nothing -> do
                         cache' <- if cacheFull cache
                                     then evictEntry cache dev
                                     else return cache
                         return (Map.insert i (b, True, ts) cache', ())
  flush          = do BCS cache _ <- get
                      forM_ (Map.toList cache) $ \ (i, (block, dirty, _)) ->
                        when dirty $ do
                          lift $ bdWriteBlock dev i block
  --
  cacheFull m = Map.size m >= size

-- |Run something in the BlockCache monad.
runBCM :: Monad m => BCM m a -> m a
runBCM m = evalStateT m (BCS Map.empty 0)

-- Run an operation that does something with the cache.
runCacheOp :: Monad m =>
              (BlockMap -> Word64 -> BCM m (BlockMap, a)) ->
              BCM m a
runCacheOp f = do
  BCS mp ts <- get
  (mp', res) <- f mp ts
  put (BCS mp' (ts + 1))
  return res

-- Evict the oldest entry from the Cache
evictEntry :: Monad m => BlockMap -> BlockDevice m -> BCM m BlockMap
evictEntry m d
  | oldestEntry == maxBound = fail "Your cache state has gone insane!"
  | dirty                   = do lift $ bdWriteBlock d oldestEntry block
                                 return $! Map.delete oldestEntry m
  | otherwise               = return $! Map.delete oldestEntry m
 where
  (oldestEntry, (block, dirty, _)) =
    Map.foldWithKey (\ i igrp@(_, _, its) old@(_, (_, _, jts)) ->
                      if its < jts then (i, igrp) else old)
                    (maxBound, (undefined, undefined, maxBound))
                    m
