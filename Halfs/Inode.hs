{-# LANGUAGE GeneralizedNewtypeDeriving, ScopedTypeVariables #-}
module Halfs.Inode
  (
    Inode(..)
  , InodeRef(..)
  , blockAddrToInodeRef
  , buildEmptyInodeEnc
  , drefInode
  , inodeRefToBlockAddr
  , nilInodeRef
  , readStream
  , writeStream
  -- * for testing
  , computeNumAddrs
  , minimalInodeSize
  )
 where

import Control.Exception
import Control.Monad
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Data.Char
import Data.List (unfoldr)
import Data.Serialize 
import Data.Serialize.Get
import Data.Serialize.Put
import Data.Word

import Halfs.BlockMap (BlockMap)
import qualified Halfs.BlockMap as BM
import Halfs.Classes
import Halfs.Errors
import Halfs.Protection
import Halfs.Utils
import System.Device.BlockDevice

import Debug.Trace


--------------------------------------------------------------------------------
-- Inode types, instances, constructors, and geometry calculation functions

-- We store Inode reference as simple Word64, newtype'd in case we either decide
-- to do something more fancy or just to make the types a bit more clear.
--
-- At this point, we assume an Inode reference is equal to its block address,
-- and we fix Inode references as Word64s. Note that if you change the
-- underlying field size of an InodeRef, you really really need to change
-- 'inodeRefSize', below.

newtype InodeRef = IR Word64
  deriving (Eq, Ord, Num, Show, Integral, Enum, Real)

instance Serialize InodeRef where
  put (IR x) = putWord64be x
  get        = IR `fmap` getWord64be

-- |Convert a disk block address into an Inode reference.
blockAddrToInodeRef :: Word64 -> InodeRef
blockAddrToInodeRef = IR

-- |Convert an inode reference into a block address
inodeRefToBlockAddr :: InodeRef -> Word64
inodeRefToBlockAddr (IR x) = x

-- The size of an Inode reference in bytes
inodeRefSize :: Int
inodeRefSize = 8

-- The nil Inode reference.  With the current Word64 representation and the
-- block layout assumptions, block 0 is the superblock, and thus an invalid
-- inode reference.
nilInodeRef :: InodeRef
nilInodeRef = IR 0

-- The structure of an Inode. Pretty standard, except that we use the
-- continuation field to allow multiple runs of block addresses within the
-- file. We serialize Nothing as nilInodeRef, an invalid continuation.
--
-- We semi-arbitrarily state that an Inode must be capable of maintaining a
-- minimum of 49 block addresses, which gives us a minimum inode size of 512
-- bytes (in the IO monad variant, which uses the our Serialize instance for the
-- UTCTime when writing the createTime and modifyTime fields).
--
minimumNumberOfBlocks :: Int
minimumNumberOfBlocks = 49

data (Eq t, Ord t, Serialize t) => Inode t = Inode {
    address       :: InodeRef        -- ^ block addr of this inode
  , parent        :: InodeRef        -- ^ block addr of parent directory inode:
                                     --   This is nilInodeRef for the root
                                     --   directory inode and for inodes in the
                                     --   continuation chain of other inodes.
  , continuation  :: Maybe InodeRef
  , createTime    :: t
  , modifyTime    :: t
  , user          :: UserID
  , group         :: GroupID
  , numAddrs      :: Word64          -- ^ number of total block addresses
                                     -- that this inode covers (DELETE_ME)
  , blockCount    :: Word64          -- ^ current number of active blocks (equal
                                     -- to the number of (sequential) inode
                                     -- references held in the blocks list)
  , blocks        :: [InodeRef]
  }
  deriving (Show, Eq)

instance (Eq t, Ord t, Serialize t) => Serialize (Inode t) where
  put n = do putByteString magic1
             put $ address n
             put $ parent n
             let contVal = case continuation n of
                             Nothing -> nilInodeRef
                             Just x  -> x
             put contVal
             put $ createTime n
             put $ modifyTime n
             putByteString magic2
             put $ user n
             put $ group n
             putWord64be $ numAddrs n
             putWord64be $ blockCount n
             putByteString magic3
             let blocks'    = blocks n
                 numAddrs'  = fromIntegral $ numAddrs n
                 numBlocks  = length blocks'
                 fillBlocks = numAddrs' - numBlocks
             unless (numBlocks <= numAddrs') $
               fail $ "Corrupted Inode structure: too many blocks"
             forM_ blocks' put
             replicateM_ fillBlocks $ put nilInodeRef
             putByteString magic4
  get   = do checkMagic magic1
             addr <- get
             par  <- get
             cntI <- get
             let cont = if inodeRefToBlockAddr cntI == 0 
                          then Nothing
                          else Just cntI
             ctm  <- get
             mtm  <- get
             unless (mtm >= ctm) $
               fail "Incoherent modified / creation times."
             checkMagic magic2
             u    <- get
             grp  <- get
             na   <- getWord64be
             lsb  <- getWord64be
             checkMagic magic3
             remb <- remaining
             let numBlockBytes      = remb - 8 -- account for trailing magic4
                 (numBlocks, check) = numBlockBytes `divMod` inodeRefSize
             unless (check == 0) $ 
               fail "Incorrect number of bytes left for block list."
             unless (numBlocks >= minimumNumberOfBlocks) $
               fail "Not enough space left for minimum number of blocks."
             blks <- filter (/= nilInodeRef) `fmap` replicateM numBlocks get
             checkMagic magic4
             return $ Inode addr par cont ctm mtm u grp na lsb blks
   where
    checkMagic x = do magic <- getBytes 8
                      unless (magic == x) $ fail "Invalid superblock."

-- | Size of a minimal inode structure when serialized, in bytes.  This will
-- vary based on the space required for type t when serialized.  Note that
-- minimal inode structure always contains minimumNumberOfBlocks InodeRefs in
-- its blocks region.
--
-- You can check this value interactively in ghci by doing, e.g.
-- minimalInodeSize =<< (getTime :: IO UTCTime)
minimalInodeSize :: (Monad m, Ord t, Serialize t) => t -> m Word64
minimalInodeSize t = do
  return $ fromIntegral $ BS.length $ encode $
    let e = emptyInode
              (fromIntegral minimumNumberOfBlocks)
              t
              t
              nilInodeRef
              nilInodeRef
              rootUser
              rootGroup
    in
      e{ blocks = replicate minimumNumberOfBlocks nilInodeRef }

-- | Computes the number of block addresses storable by an inode
computeNumAddrs :: Monad m => 
                   Word64 -- ^ block size, in bytes
                -> Word64 -- ^ minimum inode size, in bytes
                -> m Word64
computeNumAddrs blockSize minSize = do
  unless (minSize <= blockSize) $
    fail "computeNumAddrs: Block size too small to accomodate minimal inode"
  let
    -- # bytes required for the blocks region of the minimal inode
    padding       = fromIntegral $ minimumNumberOfBlocks * inodeRefSize 
    -- # bytes of the inode excluding the blocks region
    notBlocksSize = minSize - padding
    -- # bytes available for storing the blocks region
    blockSize'    = blockSize - notBlocksSize
  unless (0 == blockSize' `mod` fromIntegral inodeRefSize) $
    fail "computeNumAddrs: Inexplicably bad block size"
  return $ blockSize' `div` fromIntegral inodeRefSize

computeNumAddrsM :: (Serialize t, Timed t m) =>
                    Word64 -> m Word64
computeNumAddrsM blockSize =
  computeNumAddrs blockSize =<< minimalInodeSize =<< getTime

-- Builds and encodes an empty inode
buildEmptyInodeEnc :: (Serialize t, Timed t m) =>
                      BlockDevice m -- ^ The block device
                   -> InodeRef      -- ^ This inode's block address
                   -> InodeRef      -- ^ Parent's block address
                   -> UserID
                   -> GroupID
                   -> m ByteString
buildEmptyInodeEnc bd me mommy usr grp =
  liftM encode $ buildEmptyInode bd me mommy usr grp

buildEmptyInode :: (Serialize t, Timed t m) =>
                   BlockDevice m    -- ^ The block device
                -> InodeRef         -- ^ This inode's block address
                -> InodeRef         -- ^ Parent block's address
                -> UserID
                -> GroupID
                -> m (Inode t)
buildEmptyInode bd me mommy usr grp = do 
  now     <- getTime
  minSize <- minimalInodeSize =<< return now
  nAddrs  <- computeNumAddrs (bdBlockSize bd) minSize
  return $ emptyInode nAddrs now now me mommy usr grp

emptyInode :: (Ord t, Serialize t) => 
              Word64   -- ^ number of block addresses
           -> t        -- ^ creation time
           -> t        -- ^ last modify time
           -> InodeRef -- ^ block addr for this inode
           -> InodeRef -- ^ parent block address
           -> UserID  
           -> GroupID
           -> Inode t
emptyInode nAddrs createTm modTm me mommy usr grp =
  Inode {
    address      = me
  , parent       = mommy
  , continuation = Nothing
  , createTime   = createTm
  , modifyTime   = modTm
  , user         = usr
  , group        = grp
  , numAddrs     = nAddrs
  , blockCount   = 0
  , blocks       = []
  }


--------------------------------------------------------------------------------
-- Inode utility functions

assertValidIR :: InodeRef -> InodeRef
assertValidIR ir = assert (ir /= nilInodeRef) ir

-- | Reads the contents of the given inode's ith block
readInodeBlock :: (Ord t, Serialize t, Monad m) =>
                  BlockDevice m -> Inode t -> Word64 -> m ByteString
readInodeBlock dev n i =
  assert (i < blockCount n) $ do
  let addr = inodeRefToBlockAddr $ assertValidIR (blocks n !! fromIntegral i)
  t <- bdReadBlock dev addr
--  trace ("\nReading inode @ block addr " ++ show addr ++ ": " ++ show t) $ do
  return t

-- | Writes to the given inode's ith block
writeInodeBlock :: (Ord t, Serialize t, Monad m) =>
                   BlockDevice m -> Inode t -> Word64 -> ByteString -> m ()
writeInodeBlock dev n i bytes =
  assert (BS.length bytes == fromIntegral (bdBlockSize dev)) $ do
  let addr = inodeRefToBlockAddr $ assertValidIR (blocks n !! fromIntegral i)
  bdWriteBlock dev addr bytes

-- Writes the given inode to its block address
writeInode :: (Ord t, Serialize t, Monad m, Show t) =>
              BlockDevice m -> Inode t -> m ()
writeInode dev n =
  trace ("Writing inode: " ++ show n) $ do
  bdWriteBlock dev (inodeRefToBlockAddr $ address n) (encode n)

-- | Expands the given inode into an inode list containing itself followed by
-- all of its continuation inodes
expandConts :: (Serialize t, Timed t m, Functor m) =>
               BlockDevice m -> Inode t -> m [Inode t]
expandConts _   inode@Inode{ continuation = Nothing      } = return [inode]
expandConts dev inode@Inode{ continuation = Just nextRef } = 
  (inode:) `fmap` (drefInode dev nextRef >>= expandConts dev)

drefInode :: (Serialize t, Timed t m, Functor m) =>
             BlockDevice m -> InodeRef -> m (Inode t)
drefInode dev (IR addr) = 
  decode `fmap` bdReadBlock dev addr >>=
  either (fail . (++) "drefInode decode failure: ") return

-- | Returns Nothing on success
writeStream :: HalfsCapable b t r l m =>
               BlockDevice m -- ^ The block device
            -> BlockMap b r  -- ^ The block map
            -> InodeRef      -- ^ Starting inode ref
            -> Word64        -- ^ Starting stream (byte) offset
            -> Bool          -- ^ Truncating write?
            -> ByteString    -- ^ Data to write
            -> m (Either HalfsError ())
writeStream dev bm startIR start trunc bytes = do
  let len = fromIntegral $ BS.length bytes
  trace ("writeStream: len = " ++ show len) $ do
  if 0 == len then return $ Right () else do 
  startInode <- drefInode dev startIR

  -- TODO: Error handling: the start offset can be exactly at the end of the
  -- stream, but not beyond it so as not to introduce "gaps" -- this is
  -- currently an assertion in decompParanoid but it should be a proper error
  -- here.

  -- Compute (block) addrs per inode (api) and decompose the start byte offset
  api <- computeNumAddrsM bs
  let bpi = bs * api
  (sInodeIdx, sBlkOff, sByteOff) <- decompParanoid dev bs bpi start startInode
    -- ^^^ XXX: Replace w/ decompStreamOffset once inode growth etc. is working

  trace ("sInodeIdx, sBlkOff, sByteOff = " ++ show (sInodeIdx, sBlkOff, sByteOff)) $ do

  -- NB: expandConts is probably not viable once inode chains get large, but
  -- neither is the continuation scheme in general.  Revisit after stuff is
  -- working.
  origInodes <- expandConts dev startInode

  trace ("origInodes = " ++ show origInodes) $ do
  trace ("addrs per inode = " ++ show api) $ do

  -- Determine if we need to allocate space for the data

  -- NB: This implementation currently 'flattens' Contig/Discontig block groups
  -- from the BlockMap allocator, which will force us to treat them as Discontig
  -- when we unalloc.  We may want to have the inodes hold onto these block
  -- groups directly and split/merge them as needed to reduce the number of
  -- unallocation actions required, but we'll leave this as a TODO for now.

  let
    -- # of already-allocated bytes from the start to the end of stream 
    alreadyAllocd =
      let
        -- # of already-allocated bytes in the start inode's start block
        allocdInBlk   = if sBlkOff < blockCount startInode
                        then bs
                        else 0
        -- # of already-alloc'd bytes in the start inode after the start block
        allocdInNode  = if sBlkOff + 1 < blockCount startInode
                        then blockCount startInode - sBlkOff - 1
                        else 0
        -- # of already-alloc'd bytes in the rest of the existing inodes
        allocdInConts = sum $ map ((*bs) . blockCount) $
                         drop (fromIntegral $ sInodeIdx + 1) origInodes
      in
        trace ("allocdInBlk   = " ++ show allocdInBlk)   $ 
        trace ("allocdInNode  = " ++ show allocdInNode)  $ 
        trace ("allocdInConts = " ++ show allocdInConts) $ 
        allocdInBlk + allocdInNode + allocdInConts

  trace ("alreadyAllocd = " ++ show alreadyAllocd) $ do

  let availBlks :: forall t. (Ord t, Serialize t) => Inode t -> Word64
      availBlks n = api - blockCount n
      bytesToAlloc  = if alreadyAllocd > len then 0 else len - alreadyAllocd
      blksToAlloc   = bytesToAlloc `divCeil` bs
      inodesToAlloc = fromIntegral $
                      (blksToAlloc - availBlks (last origInodes))
                      `divCeil` api
      (u, g)        = (user startInode, group startInode)

  trace ("bytesToAlloc           = " ++ show bytesToAlloc)             $ do
  trace ("blksToAlloc            = " ++ show blksToAlloc)              $ do
  trace ("inodesToAlloc          = " ++ show inodesToAlloc)            $ do
  trace ("availBlks startInode = " ++ show (availBlks startInode)) $ do

  let doAllocs = allocAndFill availBlks blksToAlloc inodesToAlloc u g origInodes
  whenOK doAllocs $ \inodes -> do 
  trace ("inodes = " ++ show inodes) $ do
  
  -- TODO: create the first chunk and roll it in with chunks down below;
  -- same for blkAddr

  -- Write the start block, preserving the region before the start byte offset
  let stInode         = inodes !! fromIntegral sInodeIdx
      (sData, bytes') = BS.splitAt (fromIntegral $ bs - sByteOff) bytes 
  preserve <- BS.take (fromIntegral sByteOff)
              `fmap` readInodeBlock dev stInode sBlkOff
  writeInodeBlock dev stInode sBlkOff (preserve `BS.append` sData)
  trace ("Wrote start block @ addr " ++ show (blocks stInode !! fromIntegral sBlkOff)) $ do

  let
    -- Destination block addresses after the start block
    blkAddrs = map inodeRefToBlockAddr $
               drop (fromIntegral $ sBlkOff + 1) (blocks stInode)
               ++ concatMap blocks (drop (fromIntegral $ sInodeIdx + 1) inodes)
    -- Block-sized chunks
    chunks =
      unfoldr (\s -> if BS.null s
                     then Nothing
                     else
                       -- Pad last chunk up to the block size
                       let n              = fromIntegral bs
                           p@(rslt, rest) = BS.splitAt n s
                       in
                         Just $
                         if BS.null rest
                         then ( BS.take n $ rslt `BS.append` BS.replicate n 0
                              , rest
                              )
                         else p
              )
              bytes'
  assert (all ((== fromIntegral bs) . BS.length) chunks) $ do
  trace ("chunks = " ++ show chunks) $ do
  trace ("blkAddrs = " ++ show blkAddrs) $ do                                               
  trace ("len blkAddrs = " ++ show (length blkAddrs)) $ do    
  trace ("len chunks = " ++ show (length chunks)) $ do
  -- Write the remaining blocks
  mapM_ (uncurry $ bdWriteBlock dev) (blkAddrs `zip` chunks)
               
  -- Update persisted inodes from the start inode to end of write region
  mapM_ (writeInode dev) $
    let inodesUpdated = fromIntegral $ len `divCeil` bpi
    in
      trace ("inodesUpdated = " ++ show inodesUpdated) $ 
      take inodesUpdated $ drop (fromIntegral sInodeIdx) inodes

  when trunc $ do fail "Inode.writeStream: truncating write NYI" -- TODO
  return $ Right ()
  where
    whenOK act f      = act >>= either (return . Left) f
    bs                = bdBlockSize dev
    -- 
    allocBlocks n = do -- currently "flattens" BlockGroup; see above comment
      mbg <- BM.allocBlocks bm n
      case mbg of
        Nothing -> return $ Left HalfsAllocFailed
        Just bg -> return $ Right $ map blockAddrToInodeRef $ BM.blkRangeBG bg
    -- 
    -- Allocate the given number of blocks, and fill them into the inodes' block
    -- lists, allocating new inodes as needed.  The result is the final inode
    -- chain to write data into.
    allocAndFill avail blksToAlloc inodesToAlloc usr grp existingInodes = 
      -- TODO: numAddrs adjustments
      if blksToAlloc == 0
       then return (Right existingInodes)
       else do
         whenOK (allocInodes inodesToAlloc usr grp) $ \newInodes -> do
         whenOK (allocBlocks blksToAlloc)           $ \blks      -> do
         -- trace ("newInodes = " ++ show newInodes) $ do              
         -- trace ("allocated blks = " ++ show blks) $ do

         -- Fixup continuation fields and form the region that we'll fill with
         -- the newly allocated blocks (i.e., starting at the last inode but
         -- including the newly allocated inodes as well).
         let (_, region) = foldr (\inode (cont, acc) ->
                                    ( Just $ address inode
                                    , inode{ continuation = cont } : acc
                                    )
                                 )
                                 (Nothing, [])
                                 (last existingInodes : newInodes)
         -- "Spill" the allocated blocks into the empty region
         let (blks', k)                   = foldl fillBlks (blks, id) region
             inodes'                      = init existingInodes ++ k []
             fillBlks (remBlks, k') inode =
               let cnt    = min (fromIntegral $ avail inode) (length remBlks)
                   inode' =
                     inode { blockCount = blockCount inode + fromIntegral cnt
                           , blocks     = blocks inode ++ take cnt remBlks
                           }
               in
                 (drop cnt remBlks, k' . (inode':))

         assert (null blks') $ return ()
         assert (length inodes' >= length existingInodes) $ return ()
         return (Right inodes')
    -- 
    allocInodes n u g =
      if 0 == n
      then return $ Right []
      else do
        minodes <- fmap sequence $ replicateM n $ do
                     mir <- (fmap . fmap) blockAddrToInodeRef (BM.alloc1 bm)
                     case mir of
                       Nothing -> return Nothing
                       Just ir -> Just
                                  `fmap` buildEmptyInode dev ir nilInodeRef u g
        maybe (return $ Left HalfsAllocFailed) (return . Right) minodes

-- | Provides a stream over the bytes governed by a given Inode and its
-- continuations.
-- 
-- NB: This is a pretty primitive way to go about this, but it's probably
-- worthwhile to get something working before revisiting it.  In particular, if
-- this works well enough we might want to consider making this a little less
-- specific to the particulars of way that the Inode tracks its block addresses,
-- counts, continuations, etc., and perhaps build enumerators for
-- inode/block/byte sequences over inodes.
readStream :: HalfsCapable b t r l m => 
              BlockDevice m -- ^ Block device
           -> InodeRef      -- ^ Starting inode reference
           -> Word64        -- ^ Starting stream (byte) offset
           -> Maybe Word64  -- ^ Stream length (Nothing => until end)
           -> m ByteString  -- ^ Stream contents
readStream dev startIR start mlen = do
  startInode <- drefInode dev startIR
  if 0 == blockCount startInode then return BS.empty else do 
  -- Compute bytes per inode (bpi) and decompose the starting byte offset
  bpi <- (*bs) `fmap` computeNumAddrsM bs
  (sInodeIdx, sBlkOff, sByteOff) <- decompParanoid dev bs bpi start startInode
    -- ^^^ XXX: Replace w/ decompStreamOffset once inode growth etc. is working
  inodes <- expandConts dev startInode

  trace ("readStream: inodes = " ++ show inodes) $ do

  case mlen of
    Nothing  -> case drop (fromIntegral sInodeIdx) inodes of
      [] -> fail "Inode.readStream internal error: invalid start inode index"
      (inode:rest) -> do
        -- The 'header' is just the partial first block and all remaining blocks
        -- in the first inode
        header <- do
          let blkCnt = blockCount inode 
              range  = assert (sBlkOff < blkCnt) $ [sBlkOff..blkCnt - 1]
          (blk:blks) <- mapM (getBlock inode) range
          return $ BS.drop (fromIntegral sByteOff) blk
                   `BS.append` BS.concat blks

        trace ("readStream: length header = " ++ show (BS.length header)) $ do

        -- 'fullBlocks' is the remaining content from all remaining inodes
        fullBlocks <- foldM
          (\acc inode' -> do
             blks <- mapM (getBlock inode') [0..blockCount inode' - 1]
             return $ BS.concat blks `BS.append` acc
          )
          BS.empty rest
        trace ("readStream: length fullBlocks = " ++ show (BS.length fullBlocks)) $ do

        return $ header `BS.append` fullBlocks

    Just len -> do
      (_eInodeIdx, _eBlk, _eByte) <-
        decompParanoid dev bs bpi (start + len - 1) startInode
      fail "NYI: Upper-bounded inode stream read"
  where
    getBlock = readInodeBlock dev
    bs       = bdBlockSize dev

-- | Decompose the given absolute byte offset into an inode data stream into
-- inode index (i.e., 0-based index into sequence of Inode continuations), block
-- offset within that inode, and byte offset within that block.
decompStreamOffset :: (Ord t, Serialize t) =>
                      Word64  -- ^ Block size, in bytes
                   -> Word64  -- ^ Maximum number of bytes "stored" by an inode 
                   -> Word64  -- ^ Offset into the inode data stream
                   -> Inode t -- ^ The inode (for important sanity checks)
                   -> (Word64, Word64, Word64)
decompStreamOffset blkSizeBytes numBytesPerInode streamOff inode =
  -- HERE: get rid of numAddrs field?
  assert (streamOff <= numAddrs inode) (inodeIdx, blkOff, byteOff)
  where
    (inodeIdx, inodeByteIdx) = streamOff `divMod` numBytesPerInode
    (blkOff, byteOff)        = inodeByteIdx `divMod` blkSizeBytes

-- | Same as decompStreamOffset, but performs additional "heavyweight" checks
-- (e.g., reading persisted inodes from disk) on the inode structure to ensure
-- that the computed stream offset decomp is valid; use of this function can be
-- discontinued after we're sure inodes are growing correctly and the simple
-- check of numAddrs in decompStreamOffset is sufficient.
decompParanoid ::
  (Serialize t, Timed t m, Functor m) =>
     BlockDevice m 
  -> Word64  -- ^ Block size, in bytes
  -> Word64  -- ^ Maximum number of bytes "stored" by an inode 
  -> Word64  -- ^ Offset into the inode data stream
  -> Inode t -- ^ The inode (for important sanity checks)
  -> m (Word64, Word64, Word64)
decompParanoid dev blkSizeBytes numBytesPerInode streamOff inode = do 
  let (inodeIdx, blkOff, byteOff) =
        decompStreamOffset blkSizeBytes numBytesPerInode streamOff inode
  inodes <- expandConts dev inode
  assert (inodeIdx < fromIntegral (length inodes)) $ do
  let blkCnt = blockCount (inodes !! fromIntegral inodeIdx)
  assert (blkCnt == 0 && blkOff == 0 || blkOff < blkCnt) $ do
  return (inodeIdx, blkOff, byteOff)


--------------------------------------------------------------------------------
-- Magic numbers

magicStr :: String
magicStr = "This is a halfs Inode structure!"

magicBytes :: [Word8]
magicBytes = assert (length magicStr == 32) $
             map (fromIntegral . ord) magicStr

magic1, magic2, magic3, magic4 :: ByteString
magic1 = BS.pack $ take 8 $ drop  0 magicBytes
magic2 = BS.pack $ take 8 $ drop  8 magicBytes
magic3 = BS.pack $ take 8 $ drop 16 magicBytes
magic4 = BS.pack $ take 8 $ drop 24 magicBytes

