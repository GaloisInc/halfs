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
  , bsDrop
  , bsTake
  , computeNumAddrs
  , computeNumAddrsM
  , decodeInode
  , minimalInodeSize
  , safeToInt
  , truncSentinel
  )
 where

import Control.Exception
import Control.Monad
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Data.Char
import Data.List (genericDrop, genericTake, genericSplitAt)
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

dbug :: String -> a -> a
--dbug   = seq . unsafePerformIO . putStrLn
dbug _ = id


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

type StreamIdx = (Word64, Word64, Word64)

-- | Convert a disk block address into an Inode reference.
blockAddrToInodeRef :: Word64 -> InodeRef
blockAddrToInodeRef = IR

-- | Convert an inode reference into a block address
inodeRefToBlockAddr :: InodeRef -> Word64
inodeRefToBlockAddr (IR x) = x

-- | The size of an Inode reference in bytes
inodeRefSize :: Word64
inodeRefSize = 8

-- | The nil Inode reference.  With the current Word64 representation and the
-- block layout assumptions, block 0 is the superblock, and thus an invalid
-- inode reference.
nilInodeRef :: InodeRef
nilInodeRef = IR 0

-- | The sentinel byte written to partial blocks when doing truncating writes
truncSentinel :: Word8
truncSentinel = 0xBA

-- | The structure of an Inode. Pretty standard, except that we use the
-- continuation field to allow multiple runs of block addresses within the
-- file. We serialize Nothing as nilInodeRef, an invalid continuation.
--
-- We semi-arbitrarily state that an Inode must be capable of maintaining a
-- minimum of 50 block addresses, which gives us a minimum inode size of 512
-- bytes (in the IO monad variant, which uses the our Serialize instance for the
-- UTCTime when writing the createTime and modifyTime fields).
--
minimumNumberOfBlocks :: Word64
minimumNumberOfBlocks = 50

data (Eq t, Ord t, Serialize t) => Inode t = Inode
  { address       :: InodeRef        -- ^ block addr of this inode
  , parent        :: InodeRef        -- ^ block addr of parent directory inode:
                                     --   This is nilInodeRef for the root
                                     --   directory inode and for inodes in the
                                     --   continuation chain of other inodes.
  , continuation  :: Maybe InodeRef
  , createTime    :: t
  , modifyTime    :: t
  , user          :: UserID
  , group         :: GroupID
  , blockCount    :: Word64          -- ^ current number of active blocks (equal
                                     -- to the number of (sequential) inode
                                     -- references held in the blocks list)

  , blocks        :: [Word64]

  -- Fields below here are not persisted, and are populated via decodeInode

  , numAddrs      :: Word64          -- ^ Maximum number of blocks addressable
                                     -- by this inode.  NB: Does not include any
                                     -- continuation inodes, and is only used
                                     -- for sanity checking.
  }
  deriving (Show, Eq)

instance (Eq t, Ord t, Serialize t) => Serialize (Inode t) where
  put n = do
    unless (numBlocks <= numAddrs') $
      fail $ "Corrupted Inode structure put: too many blocks"

    putByteString magic1
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
    putWord64be $ blockCount n
    putByteString magic3
    forM_ blocks' put
    replicateM_ fillBlocks $ put nilInodeRef
    putByteString magic4
    where
      blocks'    = blocks n
      numAddrs'  = safeToInt $ numAddrs n
      numBlocks  = length blocks'
      fillBlocks = numAddrs' - numBlocks

  get = do
    checkMagic magic1
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
    lsb  <- getWord64be
    checkMagic magic3
    remb <- fromIntegral `fmap` remaining
    let numBlockBytes      = remb - 8 -- account for trailing magic4
        (numBlocks, check) = numBlockBytes `divMod` inodeRefSize
    unless (check == 0) $ 
      fail "Incorrect number of bytes left for block list."
    unless (numBlocks >= minimumNumberOfBlocks) $
      fail "Not enough space left for minimum number of blocks."
    blks <- filter (/= 0) `fmap` replicateM (safeToInt numBlocks) get
    checkMagic magic4
    let na = error $ "numAddrs has not been populated via Data.Serialize.get "
                  ++ "for Inode; did you forget to use the " 
                  ++ "Inode.decodeInode wrapper?"
    return $ Inode addr par cont ctm mtm u grp lsb blks na
   where
    checkMagic x = do
      magic <- getBytes 8
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
      e{ blocks = replicate (safeToInt minimumNumberOfBlocks) 0 }

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
    padding       = minimumNumberOfBlocks * inodeRefSize 
    -- # bytes of the inode excluding the blocks region
    notBlocksSize = minSize - padding
    -- # bytes available for storing the blocks region
    blockSize'    = blockSize - notBlocksSize
  unless (0 == blockSize' `mod` inodeRefSize) $
    fail "computeNumAddrs: Inexplicably bad block size"
  return $ blockSize' `div` inodeRefSize

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
  Inode
  { address      = me
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
-- Inode stream functions

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
           -> Maybe Word64  -- ^ Stream length (Nothing => until end of stream,
                            -- including entire last block)
           -> m ByteString  -- ^ Stream contents
readStream dev startIR start mlen = do
  startInode <- drefInode dev startIR
  if 0 == blockCount startInode then return BS.empty else do 
  -- Compute bytes per inode (bpi) and decompose the starting byte offset
  bpi    <- (*bs) `fmap` computeNumAddrsM bs
  inodes <- expandConts dev startInode
  dbug ("==== readStream begin ===") $ do
  (sInodeIdx, sBlkOff, sByteOff) <- decompStreamOffset bs bpi start inodes
  dbug ("start = " ++ show start) $ do
  dbug ("(sInodeIdx, sBlkOff, sByteOff) = " ++ show (sInodeIdx, sBlkOff, sByteOff)) $ do

  case mlen of
    Just len | len == 0 -> return BS.empty
    _                   -> do
      case genericDrop sInodeIdx inodes of
        [] -> fail "Inode.readStream internal error: invalid start inode index"
        (inode:rest) -> do
          -- 'header' is just the partial first block and all remaining blocks in
          -- the first inode, accounting for the possible upper bound on the length
          -- of the data returned.
          assert (maybe True (> 0) mlen) $ return ()
          header <- do
            let remBlks = calcRemBlks inode (+ sByteOff)
                          -- +sByteOff to force rounding for partial blocks
                range   = let lastIdx = blockCount inode - 1 in 
                          [ sBlkOff .. min lastIdx (sBlkOff + remBlks - 1) ]
            (blk:blks) <- mapM (readBlock inode) range
            return $ bsDrop sByteOff blk `BS.append` BS.concat blks
      
          -- 'fullBlocks' is the remaining content from all remaining inodes,
          -- accounting for the possible upper bound on the length of the data
          -- returned.
          (fullBlocks, _readCnt) <-
            foldM
              (\(acc, bytesSoFar) inode' -> do
                 let remBlks = calcRemBlks inode' (flip (-) bytesSoFar) 
                     range   = if remBlks > 0 then [0..remBlks - 1] else []
                 blks <- mapM (readBlock inode') range
                 return ( acc `BS.append` BS.concat blks
                        , bytesSoFar + remBlks * bs
                        )
              )
              (BS.empty, fromIntegral $ BS.length header) rest
          dbug ("==== readStream end ===") $ do
          return $ (maybe id bsTake mlen) $ header `BS.append` fullBlocks
  where
    readBlock = readInodeBlock dev
    bs        = bdBlockSize dev
    -- 
    -- Calculate the remaining blocks (up to len, if applicable) to read from
    -- the given inode.  f is just a length modifier.
    calcRemBlks inode f =
      case mlen of 
        Nothing  -> blockCount inode
        Just len -> min (blockCount inode) $ f len `divCeil` bs

-- | Writes to the inode stream at the given starting inode and starting byte
-- offset, overwriting data and allocating new space on disk as needed.  If the
-- write is a truncating write, all resources after the end of the written data
-- are freed.
writeStream :: HalfsCapable b t r l m =>
               BlockDevice m -- ^ The block device
            -> BlockMap b r  -- ^ The block map
            -> InodeRef      -- ^ Starting inode ref
            -> Word64        -- ^ Starting stream (byte) offset
            -> Bool          -- ^ Truncating write?
            -> ByteString    -- ^ Data to write
            -> m (Either HalfsError ())
writeStream _ _ _ _ _ bytes | 0 == BS.length bytes = return $ Right ()
writeStream dev bm startIR start trunc bytes       = do
  -- TODO: locking

  -- NB: This implementation currently 'flattens' Contig/Discontig block groups
  -- from the BlockMap allocator (see allocFill and truncUnalloc), which will
  -- force us to treat them as Discontig when we unallocate.  We may want to
  -- have the inodes hold onto these block groups directly and split/merge them
  -- as needed to reduce the number of unallocation actions required, but we'll
  -- leave this as a TODO for now.

  startInode <- drefInode dev startIR
  api        <- computeNumAddrsM bs -- (block) addrs per inode
  bpi        <- return $ bs * api   -- bytes per inode

  -- NB: expandConts is probably not viable once inode chains get large, but
  -- neither is the continuation scheme in general.  Revisit after stuff is
  -- working.
  inodes                              <- expandConts dev startInode
  sIdx@(sInodeIdx, sBlkOff, sByteOff) <- decompStreamOffset bs bpi start inodes

  -- Sanity check
  if (sInodeIdx >= fromIntegral (length inodes) ||
      let blkCnt = blockCount (inodes !! safeToInt sInodeIdx) in
      sBlkOff >= blkCnt && not (sBlkOff == 0 && blkCnt == 0)
     )
   then return $ Left $ HalfsInvalidStreamIndex start
   else do 

  dbug ("==== writeStream begin ===") $ do
  dbug ("addrs per inode           = " ++ show api)                            $ do
  dbug ("inodeIdx, blkIdx, byteIdx = " ++ show (sInodeIdx, sBlkOff, sByteOff)) $ do
  dbug ("inodes                    = " ++ show inodes)                         $ do

  -- Determine how much space we need to allocate for the data, if any
  let allocdInBlk   = if sBlkOff < blockCount startInode
                      then bs else 0
      allocdInNode  = if sBlkOff + 1 < blockCount startInode
                      then bs * (blockCount startInode - sBlkOff - 1) else 0
      allocdInConts = sum $ map ((*bs) . blockCount) $
                      genericDrop (sInodeIdx + 1) inodes
      alreadyAllocd = allocdInBlk + allocdInNode + allocdInConts
      bytesToAlloc  = if alreadyAllocd > len then 0 else len - alreadyAllocd
      blksToAlloc   = bytesToAlloc `divCeil` bs
      inodesToAlloc = (blksToAlloc - availBlks (last inodes)) `divCeil` api
      availBlks :: forall t. (Ord t, Serialize t) => Inode t -> Word64
      availBlks n   = api - blockCount n

  dbug ("alreadyAllocd = " ++ show alreadyAllocd)          $ do
  dbug ("bytesToAlloc  = " ++ show bytesToAlloc)           $ do
  dbug ("blksToAlloc   = " ++ show blksToAlloc)            $ do
  dbug ("inodesToAlloc = " ++ show inodesToAlloc)          $ do

  whenOK ( allocFill
             dev
             bm
             availBlks
             blksToAlloc
             inodesToAlloc
             (user startInode)
             (group startInode)
             inodes
         )
    $ \inodes' -> do 

  let stInode = (inodes' !! safeToInt sInodeIdx)
  sBlk <- readInodeBlock dev stInode sBlkOff

  let (sData, bytes') = bsSplitAt (bs - sByteOff) bytes
      -- The first block-sized chunk to write is the region in the start block
      -- prior to the start byte offset (header), followed by the first bytes of
      -- the data.  The trailer is nonempty and must be included when BS.length
      -- bytes < bs.
      firstChunk =
        let header   = bsTake sByteOff sBlk
            trailLen = sByteOff + fromIntegral (BS.length sData)
            trailer  = if trunc
                       then bsReplicate (bs - trailLen) truncSentinel
                       else bsDrop trailLen sBlk
            r        = header `BS.append` sData `BS.append` trailer
        in assert (fromIntegral (BS.length r) == bs) r

      -- Destination block addresses starting at the the start block
      blkAddrs = genericDrop sBlkOff (blocks stInode)
                 ++ concatMap blocks (genericDrop (sInodeIdx + 1) inodes')

  chunks <- (firstChunk:) `fmap`
            unfoldrM (getBlockContents dev trunc) (bytes', drop 1 blkAddrs)

{-
  forM chunks $ \chunk ->
    dbug ("chunk: " ++ show chunk) $ return ()
  dbug ("blkAddrs     = " ++ show blkAddrs)          $ do
  dbug ("len blkAddrs = " ++ show (length blkAddrs)) $ do
  dbug ("len chunks   = " ++ show (length chunks))   $ do
-}
  assert (all ((== safeToInt bs) . BS.length) chunks) $ do

  -- Write the remaining blocks
  mapM_ (uncurry $ bdWriteBlock dev) (blkAddrs `zip` chunks)
               
  -- If this is a truncating write, fix up the chain terminator & free all
  -- blocks & inodes in the free region
  inodes'' <- if trunc
              then truncUnalloc dev bm api sIdx len inodes'
              else return inodes'

  let inodesUpdated = len `divCeil` bpi
  dbug ("inodesUpdated = " ++ show inodesUpdated) $ return ()

  -- Update persisted inodes from the start inode to end of write region
  mapM_ (writeInode dev) $
    genericTake inodesUpdated $ genericDrop sInodeIdx inodes''

  dbug ("==== writeStream end ===") $ do
  return $ Right ()
  where
    bs           = bdBlockSize dev
    len          = fromIntegral $ BS.length bytes


--------------------------------------------------------------------------------
-- Inode stream helper & utility functions 

-- | Allocate the given number of inodes and blocks, and fill blocks into the
-- given inode chain's block lists.  Newly allocated new inodes go at the end of
-- the given inode chain, and the result is the final inode chain to write data
-- into.
allocFill :: HalfsCapable b t r l m => 
             BlockDevice m       -- ^ The block device
          -> BlockMap b r        -- ^ The block map to use for allocation
          -> (Inode t -> Word64) -- ^ Available blocks function
          -> Word64              -- ^ Number of blocks to allocate
          -> Word64              -- ^ Number of inodes to allocate
          -> UserID              -- ^ User for new inodes
          -> GroupID             -- ^ Group for new inodes
          -> [Inode t]           -- ^ Inode chain to extend and fill
          -> m (Either HalfsError [Inode t])
allocFill _ _ _ 0 _ _ _ existingInodes = return $ Right existingInodes
allocFill dev bm avail blksToAlloc inodesToAlloc usr grp existingInodes =
  whenOK (allocInodes inodesToAlloc) $ \newInodes -> do
  whenOK (allocBlocks blksToAlloc)   $ \blks      -> do
  
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
        let cnt    = min (safeToInt $ avail inode) (length remBlks)
            inode' =
              inode { blockCount = blockCount inode + fromIntegral cnt
                    , blocks     = blocks inode ++ take cnt remBlks
                    }
        in
          (drop cnt remBlks, k' . (inode':))
  
  assert (null blks') $ return ()
  assert (length inodes' >= length existingInodes) $ return ()
  return (Right inodes')
  where
    allocBlocks n = do
      -- currently "flattens" BlockGroup; see comment in writeStream
      mbg <- BM.allocBlocks bm n
      case mbg of
        Nothing -> return $ Left HalfsAllocFailed
        Just bg -> return $ Right $ BM.blkRangeBG bg
    -- 
    allocInodes n =
      if 0 == n
      then return $ Right []
      else do
        minodes <- fmap sequence $ replicateM (safeToInt n) $ do
                     mir <- (fmap . fmap) blockAddrToInodeRef (BM.alloc1 bm)
                     case mir of
                       Nothing -> return Nothing
                       Just ir -> Just
                                  `fmap`
                                  buildEmptyInode dev ir nilInodeRef usr grp
        maybe (return $ Left HalfsAllocFailed) (return . Right) minodes

-- | Truncates the stream at the given stream index and length offset, and
-- unallocates all resources in the corresponding free region
truncUnalloc :: HalfsCapable b t r l m =>
                BlockDevice m -- the block device
             -> BlockMap b r  -- the block map
             -> Word64        -- # of addrs (blocks) per inode 
             -> StreamIdx     -- starting stream index
             -> Word64        -- length at which to truncate
             -> [Inode t]     -- Current inode chain
             -> m [Inode t]   -- Truncated inode chain
truncUnalloc dev bm api sIdx len inodes = do

  dbug ("eIdx        = " ++ show eIdx)        $ return ()
  dbug ("retain'     = " ++ show retain')     $ return ()
  dbug ("freeNodes   = " ++ show freeNodes)   $ return ()
  dbug ("allFreeBlks = " ++ show allFreeBlks) $ return ()

  -- Free all of the blocks this way (as unit extents) is ugly and inefficient,
  -- but we need to be tracking BlockGroups (or reconstitute them here by
  -- looking for contiguous addresses in allFreeBlks) before we can do better.
    
  BM.unallocBlocks bm $ BM.Discontig $ map (`BM.Extent` 1) allFreeBlks
    
  -- We do not do any writes to any of the inodes that were detached from
  -- the chain & freed; this may have implications for fsck!
  return retain'
  where 
    eIdx@(eInodeIdx, eBlkOff, _) =
      addOffset api (bdBlockSize dev) (len - 1) sIdx
    (retain, freeNodes) = genericSplitAt (eInodeIdx + 1) inodes
    term                = last retain 
    freeBlks            = genericDrop (eBlkOff + 1) $ blocks term
    retain'             = init retain ++ [term']
    allFreeBlks         = freeBlks                      -- from last inode
                          ++ concatMap blocks freeNodes -- from remaining inodes
                          ++ map                        -- inode storage
                               (inodeRefToBlockAddr . address)
                               freeNodes
    term'               =
      term { continuation = Nothing
           , blockCount   = eBlkOff + 1
           , blocks       = genericTake (eBlkOff + 1) $ blocks term
           }
    

-- | Splits the input bytestring into block-sized chunks; may read from the
-- block device in order to preserve contents of blocks if needed.
getBlockContents ::
  (Monad m, Functor m) => 
    BlockDevice m
  -- ^ The block device
  -> Bool
  -- ^ Truncating write? (Impacts partial block retention)
  -> (ByteString, [Word64])
  -- ^ Input bytestring, block addresses for each chunk (for retention)
  -> m (Maybe (ByteString, (ByteString, [Word64])))
  -- ^ Remaining bytestring & chunk addrs
getBlockContents _ _ (s, _) | BS.null s    = return Nothing
getBlockContents _ _ (_, [])               = return Nothing
getBlockContents dev trunc (s, blkAddr:blkAddrs) = do
  let (newBlkData, remBytes) = bsSplitAt bs s
      bs                     = bdBlockSize dev 
  if BS.null remBytes
   then do
     -- Last block; retain the relevant portion of its data
     trailer <-
       if trunc
       then return $ bsReplicate bs truncSentinel
       else
         bsDrop (BS.length newBlkData) `fmap` bdReadBlock dev blkAddr
     let rslt = bsTake bs $ newBlkData `BS.append` trailer
     return $ Just (rslt, (remBytes, blkAddrs))
   else do
     -- Full block; nothing to see here
     return $ Just (newBlkData, (remBytes, blkAddrs))

-- | Reads the contents of the given inode's ith block
readInodeBlock :: (Ord t, Serialize t, Monad m) =>
                  BlockDevice m -> Inode t -> Word64 -> m ByteString
readInodeBlock dev n i = do 
  assert (i < blockCount n) $ return ()
  bdReadBlock dev (blocks n !! safeToInt i)

-- | Writes to the given inode's ith block
_writeInodeBlock :: (Ord t, Serialize t, Monad m) =>
                    BlockDevice m -> Inode t -> Word64 -> ByteString -> m ()
_writeInodeBlock dev n i bytes = do 
  assert (BS.length bytes == safeToInt (bdBlockSize dev)) $ return ()
  bdWriteBlock dev (blocks n !! safeToInt i) bytes

-- Writes the given inode to its block address
writeInode :: (Ord t, Serialize t, Monad m, Show t) =>
              BlockDevice m -> Inode t -> m ()
writeInode dev n =
  bdWriteBlock dev (inodeRefToBlockAddr $ address n) (encode n)

-- | Expands the given inode into an inode list containing itself followed by
-- all of its continuation inodes

-- NB/TODO: We need to optimize/fix this function. The worst case is, e.g.,
-- writing a small number of bytes at a low offset into a huge file (and hence a
-- long continuation chain): we read the entire chain when examination of the
-- stream from the start to end offsets would be sufficient.
expandConts :: (Serialize t, Timed t m, Functor m) =>
               BlockDevice m -> Inode t -> m [Inode t]
expandConts _   inode@Inode{ continuation = Nothing      } = return [inode]
expandConts dev inode@Inode{ continuation = Just nextRef } = 
  (inode:) `fmap` (drefInode dev nextRef >>= expandConts dev)

drefInode :: (Serialize t, Timed t m, Functor m) =>
             BlockDevice m -> InodeRef -> m (Inode t)
drefInode dev (IR addr) = do
  einode <- bdReadBlock dev addr >>= decodeInode (bdBlockSize dev)
  case einode of
    Left s  -> fail $ "drefInode decode failure: " ++ s
    Right r -> return r

-- | Decompose the given absolute byte offset into an inode data stream into
-- inode index (i.e., 0-based index into sequence of Inode continuations), block
-- offset within that inode, and byte offset within that block.
decompStreamOffset ::
  (Serialize t, Timed t m, Monad m) =>
     Word64    -- ^ Block size, in bytes
  -> Word64    -- ^ Maximum number of bytes "stored" by an inode 
  -> Word64    -- ^ Offset into the inode data stream
  -> [Inode t] -- ^ The inode chain, for sanity checks
  -> m StreamIdx
decompStreamOffset blkSizeBytes numBytesPerInode streamOff inodes =
  check $ return (inodeIdx, blkOff, byteOff)
  where
    (inodeIdx, inodeByteIdx) = streamOff `divMod` numBytesPerInode
    (blkOff, byteOff)        = inodeByteIdx `divMod` blkSizeBytes
    check rslt               =
      assert (inodeIdx < fromIntegral (length inodes)) $
      let blkCnt = blockCount (inodes !! safeToInt inodeIdx) in
      assert (blkCnt == 0 && blkOff == 0 || blkOff < blkCnt) $
      rslt

-- | Adds a byte offset to a (inode index, block index, byte index) triple
addOffset :: Word64                   -- max blocks per inode
          -> Word64                   -- block size, in bytes
          -> Word64                   -- byte offset
          -> (Word64, Word64, Word64) -- start index
          -> (Word64, Word64, Word64) -- offset index
addOffset blksPerInode blkSz offset (inodeIdx, blkIdx, byteIdx) =
  (inodeIdx + inodeOff + inds, blks', b)
  where
    (inodeOff, inodeByteOff) = offset `divMod` (blksPerInode * blkSz)
    (blkOff, byteOff)        = inodeByteOff `divMod` blkSz
    (blks, b)                = (byteIdx + byteOff) `divMod` blkSz
    (inds, blks')            = (blkIdx + blkOff + blks) `divMod` blksPerInode
    
-- | A wrapper around Data.Serialize.decode that populates transient fields.  We
-- do this to avoid occupying valuable on-disk inode space where possible.  Bare
-- applications of 'decode' should not occur when deserializing inodes.
decodeInode :: (Serialize t, Timed t m, Monad m) =>
               Word64
            -> ByteString
            -> m (Either String (Inode t))
decodeInode blkSize bs = do
  numAddrs' <- computeNumAddrsM blkSize
  case decode bs of
    Left s  -> return $ Left  $ s
    Right n -> return $ Right $ n { numAddrs = numAddrs' }

-- "Safe" (i.e., emits runtime assertions on overflow) versions of
-- BS.{take,drop,replicate}.  We want the efficiency of these functions without
-- the danger of an unguarded fromIntegral on the Word64 types we use throughout
-- this module, as this could overflow for absurdly large device geometries.  We
-- may need to revisit some implementation decisions should this occur (e.g.,
-- because many Prelude and Data.ByteString functions yield and take values of
-- type Int).

safeToInt :: Integral a => a -> Int
safeToInt n =
  assert (toInteger n <= toInteger (maxBound :: Int)) $ fromIntegral n

makeSafeIntF :: Integral a =>  (Int -> b) -> a -> b
makeSafeIntF f n = f $ safeToInt n

-- | "Safe" version of Data.ByteString.take
bsTake :: Integral a => a -> ByteString -> ByteString
bsTake = makeSafeIntF BS.take

-- | "Safe" version of Data.ByteString.drop
bsDrop :: Integral a => a -> ByteString -> ByteString
bsDrop = makeSafeIntF BS.drop

-- | "Safe" version of Data.ByteString.replicate
bsReplicate :: Integral a => a -> Word8 -> ByteString
bsReplicate = makeSafeIntF BS.replicate

bsSplitAt :: Integral a => a -> ByteString -> (ByteString, ByteString)
bsSplitAt = makeSafeIntF BS.splitAt

whenOK :: Monad m => m (Either a b) -> (b -> m (Either a c)) -> m (Either a c)
whenOK act f = act >>= either (return . Left) f


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

