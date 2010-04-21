{-# LANGUAGE GeneralizedNewtypeDeriving, ScopedTypeVariables #-}
module Halfs.Inode
  (
    InodeRef(..)
  , blockAddrToInodeRef
  , buildEmptyInodeEnc
  , decLinkCount
  , fileStat
  , incLinkCount
  , inodeKey
  , inodeRefToBlockAddr
  , nilInodeRef
  , readStream
  , writeStream
  -- * for internal use only!
  , atomicModifyInode
  , atomicReadInode
  , bsReplicate
  , drefInode
  , expandConts -- for use by fsck
  , fileStat_lckd
  , freeInode
  , withLockedInode
  , writeStream_lckd
  -- * for testing: ought not be used by actual clients of this module!
  , Inode(..)
  , Cont(..)
  , ContRef(..)
  , bsDrop
  , bsTake
  , computeNumAddrs
  , computeNumInodeAddrsM
  , computeNumContAddrsM
  , decodeCont
  , decodeInode
  , getSizes
  , minimalContSize
  , minimalInodeSize
  , minInodeBlocks
  , minContBlocks
  , nilContRef
  , safeToInt
  , truncSentinel
  )
 where

import Control.Exception
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Data.Char
import Data.List (find, genericDrop, genericLength, genericTake, genericSplitAt)
import Data.Maybe
import Data.Serialize 
import Data.Serialize.Get
import Data.Serialize.Put
import Data.Word

import Halfs.BlockMap (BlockMap)
import qualified Halfs.BlockMap as BM
import Halfs.Classes
import Halfs.Errors
import Halfs.HalfsState
import Halfs.Protection
import Halfs.Monad
import Halfs.MonadUtils
import Halfs.Types
import Halfs.Utils

import System.Device.BlockDevice

--import Debug.Trace
dbug :: String -> a -> a
dbug _ = id
--dbug = trace

type HalfsM b r l m a = HalfsT HalfsError (Maybe (HalfsState b r l m)) m a

--------------------------------------------------------------------------------
-- Inode/Cont constructors, geometry calculation, and helpful constructors

type StreamIdx = (Word64, Word64, Word64)

-- | Obtain a 64 bit "key" for an inode; useful for building maps etc.
-- For now, this is the same as inodeRefToBlockAddr, but clients should
-- be using this function rather than inodeRefToBlockAddr in case the
-- underlying inode representation changes.
inodeKey :: InodeRef -> Word64
inodeKey = inodeRefToBlockAddr

-- | Convert a disk block address into an Inode reference.
blockAddrToInodeRef :: Word64 -> InodeRef
blockAddrToInodeRef = IR

-- | Convert an inode reference into a block address
inodeRefToBlockAddr :: InodeRef -> Word64
inodeRefToBlockAddr = unIR

-- | The nil Inode reference.  With the current Word64 representation and the
-- block layout assumptions, block 0 is the superblock, and thus an invalid
-- Inode reference.
nilInodeRef :: InodeRef
nilInodeRef = IR 0

-- | The nil Cont reference.  With the current Word64 representation and
-- the block layout assumptions, block 0 is the superblock, and thus an
-- invalid Cont reference.
nilContRef :: ContRef
nilContRef = CR 0

-- | The sentinel byte written to partial blocks when doing truncating writes
truncSentinel :: Word8
truncSentinel = 0xBA

-- | The sentinel byte written to the padding region at the end of BlockCarriers
padSentinel :: Word8
padSentinel = 0xAD

-- We semi-arbitrarily state that an Inode must be capable of maintaining a
-- minimum of 39 block addresses in its embedded Cont while the Cont must be
-- capable of maintaining 57 block addresses.  These values, together with
-- specific padding values for inodes and conts (4 and 0, respectively), give us
-- a minimum inode AND cont size of 512 bytes each (in the IO monad variant,
-- which uses the our Serialize instance for the UTCTime when writing the time
-- fields).
--
-- These can be adjusted as needed according to inode metadata sizes, but be
-- sure to ensure that (minimalInodeSize =<< getTime) and minimalContSize yield
-- the same value!

-- | The size, in bytes, of the padding region at the end of Inodes
iPadSize :: Int
iPadSize = 4

-- | The size, in bytes, of the padding region at the end of Conts
cPadSize :: Int
cPadSize = 0

minInodeBlocks :: Word64
minInodeBlocks = 37

minContBlocks :: Word64
minContBlocks = 57

-- | The structure of an Inode. Pretty standard, except that we use the Cont
-- structure (the first of which is embedded in the inode) to hold block
-- references and use its continuation field to allow multiple runs of block
-- addresses.
data (Eq t, Ord t, Serialize t) => Inode t = Inode
  { inoParent        :: InodeRef -- ^ block addr of parent directory inode:
                                 --   This is nilInodeRef for the root
                                 --   directory inode
  -- begin fstat metadata
  , inoAddress       :: InodeRef -- ^ block addr of this inode
  , inoFileSize      :: Word64
  , inoAllocBlocks   :: Word64   -- ^ number of blocks allocated to this inode
                                 --   (includes its own allocated block, blocks
                                 --   allocated for Conts, and and all blocks in
                                 --   the cont chain itself)
  , inoFileType      :: FileType 
  , inoMode          :: FileMode
  , inoNumLinks      :: Word64   -- ^ number of hardlinks to this inode
  , inoCreateTime    :: t        -- ^ time of creation
  , inoModifyTime    :: t        -- ^ time of last data modification
  , inoAccessTime    :: t        -- ^ time of last data access
  , inoChangeTime    :: t        -- ^ time of last change to inode data 
  , inoUser          :: UserID   -- ^ userid of inode's owner
  , inoGroup         :: GroupID  -- ^ groupid of inode's owner
  -- end fstat metadata

  , inoCont          :: Cont  
  }
  deriving (Show, Eq)

data Cont = Cont
  { address      :: ContRef  -- ^ Address of this cont (nilContRef for an
                             --   inode's embedded Cont)
  , continuation :: ContRef  -- ^ Next Cont in the chain; nilContRef terminates
  , blockCount   :: Word64   
  , blockAddrs   :: [Word64] -- ^ references to blocks governed by this Cont

  -- Fields below here are not persisted, and are populated via decodeCont  

  , numAddrs     :: Word64   -- ^ Maximum number of blocks addressable by this
                             --   Cont.  NB: Does not include any continuations
  }
  deriving (Show, Eq)
               
-- | Size of a minimal inode structure when serialized, in bytes.  This will
-- vary based on the space required for type t when serialized.  Note that
-- minimal inode structure always contains minInodeBlocks InodeRefs in
-- its blocks region.
--
-- You can check this value interactively in ghci by doing, e.g.
-- minimalInodeSize =<< (getTime :: IO UTCTime)
minimalInodeSize :: (Monad m, Ord t, Serialize t, Show t) => t -> m Word64
minimalInodeSize t = do
  return $ fromIntegral $ BS.length $ encode $
    let e = emptyInode minInodeBlocks t RegularFile (FileMode [] [] [])
                       nilInodeRef nilInodeRef rootUser rootGroup
        c = inoCont e
    in e{ inoCont = c{ blockAddrs = replicate (safeToInt minInodeBlocks) 0 } }

-- | The size of a minimal Cont structure when serialized, in bytes.
minimalContSize :: Monad m => m (Word64)
minimalContSize = return $ fromIntegral $ BS.length $ encode $
  (emptyCont minContBlocks nilContRef){
    blockAddrs = replicate (safeToInt minContBlocks) 0
  }

-- | Computes the number of block addresses storable by an inode/cont
computeNumAddrs :: Monad m => 
                   Word64 -- ^ block size, in bytes
                -> Word64 -- ^ minimum number of blocks for inode/cont
                -> Word64 -- ^ minimum inode/cont total size, in bytes
                -> m Word64
computeNumAddrs blkSz minBlocks minSize = do
  unless (minSize <= blkSz) $
    fail "computeNumAddrs: Block size too small to accomodate minimal inode"
  let
    -- # bytes required for the blocks region of the minimal inode
    padding       = minBlocks * refSize
    -- # bytes of the inode excluding the blocks region
    notBlocksSize = minSize - padding
    -- # bytes available for storing the blocks region
    blkSz'    = blkSz - notBlocksSize
  unless (0 == blkSz' `mod` refSize) $
    fail "computeNumAddrs: Inexplicably bad block size"
  return $ blkSz' `div` refSize

computeNumInodeAddrsM :: (Serialize t, Timed t m, Show t) =>
                         Word64 -> m Word64
computeNumInodeAddrsM blkSz =
  computeNumAddrs blkSz minInodeBlocks =<< minimalInodeSize =<< getTime

computeNumContAddrsM :: (Serialize t, Timed t m) =>
                        Word64 -> m Word64
computeNumContAddrsM blkSz = do
  minSize <- minimalContSize
  computeNumAddrs blkSz minContBlocks minSize

getSizes :: (Serialize t, Timed t m, Show t) =>
            Word64
         -> m ( Word64 -- #inode bytes
              , Word64 -- #cont bytes
              , Word64 -- #inode addrs
              , Word64 -- #cont addrs
              )
getSizes blkSz = do
  startContAddrs <- computeNumInodeAddrsM blkSz
  contAddrs      <- computeNumContAddrsM  blkSz
  return (startContAddrs * blkSz, contAddrs * blkSz, startContAddrs, contAddrs)

-- Builds and encodes an empty inode
buildEmptyInodeEnc :: (Serialize t, Timed t m, Show t) =>
                      BlockDevice m -- ^ The block device
                   -> FileType      -- ^ This inode's filetype
                   -> FileMode      -- ^ This inode's access mode
                   -> InodeRef      -- ^ This inode's block address
                   -> InodeRef      -- ^ Parent's block address
                   -> UserID
                   -> GroupID
                   -> m ByteString
buildEmptyInodeEnc bd ftype fmode me mommy usr grp =
  liftM encode $ buildEmptyInode bd ftype fmode me mommy usr grp

buildEmptyInode :: (Serialize t, Timed t m, Show t) =>
                   BlockDevice m
                -> FileType      -- ^ This inode's filetype
                -> FileMode      -- ^ This inode's access mode
                -> InodeRef      -- ^ This inode's block address
                -> InodeRef      -- ^ Parent block's address
                -> UserID        -- ^ This inode's owner's userid
                -> GroupID       -- ^ This inode's owner's groupid
                -> m (Inode t)
buildEmptyInode bd ftype fmode me mommy usr grp = do 
  now     <- getTime
  minSize <- minimalInodeSize =<< return now
  nAddrs  <- computeNumAddrs (bdBlockSize bd) minInodeBlocks minSize
  return $ emptyInode nAddrs now ftype fmode me mommy usr grp

emptyInode :: (Ord t, Serialize t) => 
              Word64   -- ^ number of block addresses
           -> t        -- ^ creation timestamp
           -> FileType -- ^ inode's filetype
           -> FileMode -- ^ inode's access mode
           -> InodeRef -- ^ block addr for this inode
           -> InodeRef -- ^ parent block address
           -> UserID  
           -> GroupID
           -> Inode t
emptyInode nAddrs now ftype fmode me mommy usr grp =
  Inode
  { inoParent       = mommy
  , inoAddress      = me
  , inoFileSize     = 0
  , inoAllocBlocks  = 1
  , inoFileType     = ftype
  , inoMode         = fmode
  , inoNumLinks     = 1
  , inoCreateTime   = now
  , inoModifyTime   = now
  , inoAccessTime   = now
  , inoChangeTime   = now
  , inoUser         = usr
  , inoGroup        = grp
  , inoCont         = emptyCont nAddrs nilContRef 
  }

buildEmptyCont :: (Serialize t, Timed t m) =>
                  BlockDevice m -- ^ The block device
               -> ContRef       -- ^ This cont's block address
               -> m Cont
buildEmptyCont bd me = do
  minSize <- minimalContSize
  nAddrs  <- computeNumAddrs (bdBlockSize bd) minContBlocks minSize
  return $ emptyCont nAddrs me

emptyCont :: Word64  -- ^ number of block addresses
          -> ContRef -- ^ block addr for this cont
          -> Cont
emptyCont nAddrs me =
  Cont
  { address      = me
  , continuation = nilContRef
  , blockCount   = 0
  , blockAddrs   = []
  , numAddrs     = nAddrs
  }


--------------------------------------------------------------------------------
-- Inode stream functions

-- | Provides a stream over the bytes governed by a given Inode and its
-- continuations.  This function performs a write to update inode metadata
-- (e.g., access time).
readStream :: HalfsCapable b t r l m => 
              InodeRef                  -- ^ Starting inode reference
           -> Word64                    -- ^ Starting stream (byte) offset
           -> Maybe Word64              -- ^ Stream length (Nothing => read
                                        --   until end of stream, including
                                        --   entire last block)
           -> HalfsM b r l m ByteString -- ^ Stream contents
readStream startIR start mlen = 
  withLockedInode startIR $ do  
  -- ====================== Begin inode critical section ======================
  dev <- hasks hsBlockDev
  let bs        = bdBlockSize dev
      readB n b = lift $ readBlock dev n b
      -- 
      -- Calculate the remaining blocks (up to len, if applicable) to read from
      -- the given Cont.  f is just a length modifier.
      calcRemBlks cont f = case mlen of 
        Nothing  -> blockCount cont
        Just len -> min (blockCount cont) $ f len `divCeil` bs

  startInode <- drefInode dev startIR
  let fileSz  = inoFileSize startInode
  if 0 == blockCount (inoCont startInode)
   then return BS.empty
   else do 
     dbug ("==== readStream begin ===") $ do
     conts                         <- expandConts dev (inoCont startInode)
     (sContIdx, sBlkOff, sByteOff) <- getStreamIdx bs fileSz start
     dbug ("start = " ++ show start) $ do
     dbug ("(sContIdx, sBlkOff, sByteOff) = " ++
       show (sContIdx, sBlkOff, sByteOff)) $ do

     rslt <- case mlen of
       Just len | len == 0 -> return BS.empty
       _                   -> do
         case genericDrop sContIdx conts of
           [] -> fail "Inode.readStream INTERNAL: invalid start cont index"
           (cont:rest) -> do
             -- 'header' is just the partial first block and all remaining
             -- blocks in the first Cont, accounting for the (Maybe) maximum
             -- length requested.
             assert (maybe True (> 0) mlen) $ return ()
             header <- do
               let remBlks = calcRemBlks cont (+ sByteOff)
                             -- +sByteOff to force rounding for partial blocks
                   range   = let lastIdx = blockCount cont - 1 in 
                             [ sBlkOff .. min lastIdx (sBlkOff + remBlks - 1) ]
               (blk:blks) <- mapM (readB cont) range
               return $ bsDrop sByteOff blk `BS.append` BS.concat blks
                       
             -- 'fullBlocks' is the remaining content from all remaining conts,
             -- accounting for (Maybe) maximum length requested
             (fullBlocks, _readCnt) <- 
               foldM
                 (\(acc, bytesSoFar) cont' -> do
                    let remBlks = calcRemBlks cont' (flip (-) bytesSoFar) 
                        range   = if remBlks > 0 then [0..remBlks - 1] else []
                    blks <- mapM (readB cont') range
                    return ( acc `BS.append` BS.concat blks
                           , bytesSoFar + remBlks * bs
                           )
                 )
                 (BS.empty, fromIntegral $ BS.length header) rest
             return $ bsTake (maybe (fileSz - start) id mlen) $
               header `BS.append` fullBlocks
     
     now <- getTime
     lift $ writeInode dev $
       startInode { inoAccessTime = now, inoChangeTime = now }
     dbug ("==== readStream end ===") $ return ()
     return rslt                                         
  -- ======================= End inode critical section =======================

-- | Writes to the inode stream at the given starting inode and starting byte
-- offset, overwriting data and allocating new space on disk as needed.  If the
-- write is a truncating write, all resources after the end of the written data
-- are freed.  Whenever the data to be written exceeds the the end of the
-- stream, the trunc flag is ignored.
writeStream :: HalfsCapable b t r l m =>
               InodeRef           -- ^ Starting inode ref
            -> Word64             -- ^ Starting stream (byte) offset
            -> Bool               -- ^ Truncating write?
            -> ByteString         -- ^ Data to write
            -> HalfsM b r l m ()
writeStream _ _ False bytes | 0 == BS.length bytes = return ()
writeStream startIR start trunc bytes              = do
  dev <- hasks hsBlockDev
  bm  <- hasks hsBlockMap
  withLockedInode startIR $ writeStream_lckd dev bm startIR start trunc bytes

writeStream_lckd :: HalfsCapable b t r l m =>
                    BlockDevice m
                 -> BlockMap b r l
                 -> InodeRef           -- ^ Starting inode ref
                 -> Word64             -- ^ Starting stream (byte) offset
                 -> Bool               -- ^ Truncating write?
                 -> ByteString         -- ^ Data to write
                 -> HalfsM b r l m ()
writeStream_lckd _ _ _ _ False bytes | 0 == BS.length bytes = return ()
writeStream_lckd dev bm startIR start trunc bytes           = {-# SCC "myWriteStream" #-} do
  -- ====================== Begin inode critical section ======================

  -- NB: The implementation currently 'flattens' Contig/Discontig block groups
  -- from the BlockMap allocator (see allocFill and truncUnalloc), which will
  -- force us to treat them as Discontig when we unallocate.  We may want to
  -- have the Conts hold onto these block groups directly and split/merge them
  -- as needed to reduce the number of unallocation actions required, but we'll
  -- leave this as a TODO for now.

  dbug ("==== writeStream begin ===") $ do

  startInode     <- drefInode dev startIR
  let fileSz      = inoFileSize startInode
  (_, _, _, apc) <- getSizes bs

  -- NB: expandConts is probably not viable once cont chains get large, but the
  -- continuation scheme in general may not be viable.  Revisit after stuff is
  -- working.
  conts0                        <- expandConts dev (inoCont startInode)
  (sContIdx, sBlkOff, sByteOff) <- getStreamIdx bs fileSz start

  dbug ("(sContIdx, sBlkOff, sByteOff) = " ++ show (sContIdx, sBlkOff, sByteOff)) $ do
  dbug ("conts0                        = " ++ show conts0                       ) $ do
  dbug ("blockCount stCont0            = " ++ show (blockCount (conts0 !! safeToInt sContIdx))) $ do

  let newFileSz    = if trunc then start + len else max (start + len) fileSz
      fileSzRndBlk = (fileSz `divCeil` bs) * bs
      bytesToAlloc = if newFileSz > fileSzRndBlk
                      then newFileSz - fileSzRndBlk
                      else 0 
      blksToAlloc  = bytesToAlloc `divCeil` bs
      contsToAlloc = (blksToAlloc - availBlks (last conts0)) `divCeil` apc
      availBlks c  = numAddrs c - blockCount c

  dbug ("len          = " ++ show len)           $ do
  dbug ("trunc        = " ++ show trunc)         $ do
  dbug ("fileSz       = " ++ show fileSz)        $ do
  dbug ("newFileSz    = " ++ show newFileSz)     $ do
  dbug ("fileSzRndBlk = " ++ show fileSzRndBlk)  $ do
  dbug ("bytesToAlloc = " ++ show bytesToAlloc)  $ do
  dbug ("blksToAlloc  = " ++ show blksToAlloc)   $ do
  dbug ("contsToAlloc = " ++ show contsToAlloc)  $ do

  (conts1, allocDirtyConts) <-
    allocFill dev bm availBlks blksToAlloc contsToAlloc conts0

  let stCont1 = conts1 !! safeToInt sContIdx

  when (sBlkOff < blockCount stCont1) $ do 
    sBlk <- lift $ readBlock dev stCont1 sBlkOff
  
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
        blkAddrs = genericDrop sBlkOff (blockAddrs stCont1)
                   ++ concatMap blockAddrs (genericDrop (sContIdx + 1) conts1)
  
    chunks <- (firstChunk:) `fmap`
              unfoldrM (lift . getBlockContents dev trunc)
                       (bytes', drop 1 blkAddrs)
  
    assert (all ((== safeToInt bs) . BS.length) chunks) $ do
  
    -- Write the data into the appropriate blocks
    mapM_ (\(a,d) -> lift $ bdWriteBlock dev a d) (blkAddrs `zip` chunks)

  -- If this is a truncating write where we're not growing the region, free all
  -- blocks/Conts beyond in the leftover region endpoint and fix up the chain
  -- terminator.
  (conts2, unallocDirtyConts, numBlksFreed) <-
    if trunc && bytesToAlloc == 0
    then truncUnalloc dev bm start len conts1
    else return (conts1, [], 0)
  assert (length conts2 > 0 && length conts2 <= length conts1) $ return ()

  assert (null allocDirtyConts || null unallocDirtyConts) $ return ()
  let dirtyConts = allocDirtyConts ++ unallocDirtyConts
      dirtyInode = case find isEmbedded dirtyConts of
                     Nothing -> startInode
                     Just c  -> startInode{ inoCont = c }

  -- Persist all non-embedded, dirty conts
  dbug ("dirtyConts = " ++ show dirtyConts) $ return ()
  forM_ dirtyConts $ \c -> when (not $ isEmbedded c) $ lift $ writeCont dev c

  -- Metadata stuff
  dbug ("writeStream: inoAllocBlocks dirtyInode = " ++
    show (inoAllocBlocks dirtyInode) ++ ", blksToAlloc = " ++
    show blksToAlloc ++ ", contsToAlloc = " ++ show contsToAlloc ++
    ", numBlksFreed = " ++ show numBlksFreed) $ do

  assert (blksToAlloc + contsToAlloc == 0 || numBlksFreed == 0) $ return ()
  let newBlockCount = inoAllocBlocks dirtyInode
                      + blksToAlloc + contsToAlloc - numBlksFreed

  -- Finally, update inode metadata and persist it
  now <- getTime 
  lift $ writeInode dev $
    dirtyInode { inoFileSize    = newFileSz
               , inoAllocBlocks = newBlockCount
               , inoAccessTime  = now
               , inoModifyTime  = now
               , inoChangeTime  = now
               }
  dbug ("==== writeStream end ===") $ return ()
  -- ======================= End inode critical section =======================
  where
    isEmbedded  = (==) nilContRef . address
    bs          = bdBlockSize dev
    len         = fromIntegral $ BS.length bytes


--------------------------------------------------------------------------------
-- Inode operations

incLinkCount :: HalfsCapable b t r l m =>
                InodeRef -- ^ Source inode ref
             -> HalfsM b r l m ()
incLinkCount inr =
  atomicModifyInode inr $ \nd ->
    return $ nd{ inoNumLinks = inoNumLinks nd + 1 }

decLinkCount :: HalfsCapable b t r l m =>
                InodeRef -- ^ Source inode ref
             -> HalfsM b r l m ()
decLinkCount inr =
  atomicModifyInode inr $ \nd ->
    return $ nd{ inoNumLinks = inoNumLinks nd - 1 }

-- | Atomically modifies an inode; always updates inoChangeTime, but
-- callers are responsible for other metadata modifications.
atomicModifyInode :: HalfsCapable b t r l m =>
                     InodeRef
                  -> (Inode t -> HalfsM b r l m (Inode t))
                  -> HalfsM b r l m ()
atomicModifyInode inr f = 
  withLockedInode inr $ do
    dev    <- hasks hsBlockDev
    inode  <- drefInode dev inr
    now    <- getTime
    inode' <- setChangeTime now `fmap` f inode
    lift $ writeInode dev inode'

atomicReadInode :: HalfsCapable b t r l m =>
                    InodeRef
                -> (Inode t -> a)
                -> HalfsM b r l m a
atomicReadInode inr f = do
  dev <- hasks hsBlockDev
  withLockedInode inr $ f `fmap` drefInode dev inr

fileStat :: HalfsCapable b t r l m =>
            InodeRef
         -> HalfsM b r l m (FileStat t)
fileStat inr = do
  dev <- hasks hsBlockDev
  withLockedInode inr $ fileStat_lckd dev inr

fileStat_lckd :: HalfsCapable b t r l m =>
                 BlockDevice m
              -> InodeRef
              -> HalfsM b r l m (FileStat t)
fileStat_lckd dev inr = do
  inode <- drefInode dev inr
  return $ FileStat
    { fsInode      = inr
    , fsType       = inoFileType    inode
    , fsMode       = inoMode        inode
    , fsNumLinks   = inoNumLinks    inode
    , fsUID        = inoUser        inode
    , fsGID        = inoGroup       inode
    , fsSize       = inoFileSize    inode
    , fsNumBlocks  = inoAllocBlocks inode
    , fsAccessTime = inoAccessTime  inode
    , fsModifyTime = inoModifyTime  inode
    , fsChangeTime = inoChangeTime  inode
    }


--------------------------------------------------------------------------------
-- Inode/Cont stream helper & utility functions 

freeInode :: HalfsCapable b t r l m =>
             InodeRef -- ^ reference to the inode to remove
          -> HalfsM b r l m ()
freeInode inr@(IR addr) = 
  withLockedInode inr $ do
    dev <- hasks hsBlockDev
    bm  <- hasks hsBlockMap
    conts <- expandConts dev =<< inoCont `fmap` drefInode dev inr
    lift $ BM.unallocBlocks bm $ BM.Discontig $ map (`BM.Extent` 1) $
      concatMap blockAddrs conts ++ map (unCR . address) (tail conts)
      -- ^ all blocks in all conts; ^ blocks for non-embedded cont storage
    BM.unalloc1 bm addr

withLockedInode :: HalfsCapable b t r l m =>
                   InodeRef         -- ^ reference to inode to lock
                -> HalfsM b r l m a -- ^ action to take while holding lock
                -> HalfsM b r l m a
withLockedInode inr act =
  -- Inode locking: We currently use a single reader/writer lock tracked by the
  -- InodeRef -> (lock, ref count) map in HalfsState. Reference counting is used
  -- to determine when it is safe to remove a lock from the map.
  --
  -- We use the map to track lock info so that we don't hold locks for lengthy
  -- intervals when we have access requests for disparate inode refs.

  hbracket before after (const act {- inode lock doesn't escape! -}) 

  where
    before = do
      -- When the InodeRef is already in the map, atomically increment its
      -- reference count and acquire; otherwise, create a new lock and acquire.
      inodeLock <- do
        lm <- hasks hsInodeLockMap       
        withLockedRscRef lm $ \mapRef -> do
          -- begin inode lock map critical section
          lockInfo <- lookupRM inr mapRef
          case lockInfo of
            Nothing -> do
              l <- newLock
              insertRM inr (l, 1) mapRef
              return l
            Just (l, r) -> do
              insertRM inr (l, r + 1) mapRef
              return l
          -- end inode lock map critical section
      lock inodeLock
      return inodeLock
    --
    after inodeLock = do
      -- Atomically decrement the reference count for the InodeRef and then
      -- release the lock
      lm <- hasks hsInodeLockMap
      withLockedRscRef lm $ \mapRef -> do
        -- begin inode lock map critical section
        lockInfo <- lookupRM inr mapRef
        case lockInfo of
          Nothing -> fail "withLockedInode internal: No InodeRef in lock map"
          Just (l, r) | r == 1    -> deleteRM inr mapRef
                      | otherwise -> insertRM inr (l, r - 1) mapRef
        -- end inode lock map critical section
      release inodeLock

-- | A wrapper around Data.Serialize.decode that populates transient fields.  We
-- do this to avoid occupying valuable on-disk inode space where possible.  Bare
-- applications of 'decode' should not occur when deserializing inodes!
decodeInode :: HalfsCapable b t r l m =>
               Word64
            -> ByteString
            -> HalfsM b r l m (Inode t)
decodeInode blkSz bs = do
  numAddrs' <- computeNumInodeAddrsM blkSz
  case decode bs of
    Left s  -> throwError $ HE_DecodeFail_Inode s
    Right n -> do 
      return n{ inoCont = (inoCont n){ numAddrs = numAddrs' } }

-- | A wrapper around Data.Serialize.decode that populates transient fields.  We
-- do this to avoid occupying valuable on-disk Cont space where possible.  Bare
-- applications of 'decode' should not occur when deserializing Conts!
decodeCont :: HalfsCapable b t r l m =>
              Word64
           -> ByteString
           -> HalfsM b r l m Cont
decodeCont blkSz bs = do
  numAddrs' <- computeNumContAddrsM blkSz
  case decode bs of
    Left s  -> throwError $ HE_DecodeFail_Cont s
    Right c -> return c{ numAddrs = numAddrs' }

-- | Allocate the given number of Conts and blocks, and fill blocks into the
-- given inode chain's block lists.  Newly allocated new conts go at the end of
-- the given cont chain, and the result is the final cont chain to write data
-- into.
allocFill ::
  HalfsCapable b t r l m => 
     BlockDevice m                    -- ^ The block device
  -> BlockMap b r l                   -- ^ The block map to use for allocation
  -> (Cont -> Word64)                 -- ^ Available blocks function
  -> Word64                           -- ^ Number of blocks to allocate
  -> Word64                           -- ^ Number of conts to allocate
  -> [Cont]                           -- ^ Chain to extend and fill
  -> HalfsM b r l m ([Cont], [Cont])  -- ^ Extended cont chain, terminating
                                      -- subchain of dirty conts, number of
                                      -- blocks allocated
allocFill _   _  _     0           _            existing = return (existing, [])
allocFill dev bm avail blksToAlloc contsToAlloc existing = do
  newConts <- allocConts 
  blks     <- allocBlocks
  return []
  -- Fixup continuation fields and form the region that we'll fill with the
  -- newly allocated blocks (i.e., starting at the last cont but including the
  -- newly allocated conts as well).
  let (_, region) = foldr (\c (contAddr, acc) ->
                             ( address c
                             , c{ continuation = contAddr } : acc
                             )
                          )
                          (nilContRef, [])
                          (last existing : newConts)
  -- "Spill" the allocated blocks into the empty region
  let (blks', k)               = foldl fillBlks (blks, id) region
      dirtyConts               = k []
      newChain                 = init existing ++ dirtyConts
      fillBlks (remBlks, k') c =
        let cnt    = min (safeToInt $ avail c) (length remBlks)
            c'     = c { blockCount = blockCount c + fromIntegral cnt
                       , blockAddrs = blockAddrs c ++ take cnt remBlks
                       }
        in
          (drop cnt remBlks, k' . (c':))
  
  assert (null blks') $ return ()
  assert (length newChain >= length existing) $ return ()
  return (newChain, dirtyConts)
  where
    allocBlocks = do
      -- currently "flattens" BlockGroup; see comment in writeStream
      mbg <- lift $ BM.allocBlocks bm blksToAlloc
      case mbg of
        Nothing -> dbug ("allocBlocks alloc fail") $ throwError HE_AllocFailed
        Just bg -> return $ BM.blkRangeBG bg
    -- 
    allocConts =
      if 0 == contsToAlloc
      then return []
      else do
        -- TODO: Catch allocation errors and unalloc partial allocs?
        mconts <- fmap sequence $ replicateM (safeToInt contsToAlloc) $ do
          mcr <- fmap CR `fmap` BM.alloc1 bm
          case mcr of
            Nothing -> return Nothing
            Just cr -> Just `fmap` lift (buildEmptyCont dev cr)
        maybe (dbug ("allocConts alloc fail") $ throwError HE_AllocFailed)
              (return)
              mconts

-- | Truncates the stream at the given a stream index and length offset, and
-- unallocates all resources in the corresponding free region
truncUnalloc ::
  HalfsCapable b t r l m =>
     BlockDevice m                           -- ^ the block device
  -> BlockMap b r l                          -- ^ the block map
  -> Word64                                  -- ^ starting stream byte index
  -> Word64                                  -- ^ length from start at which to
                                             -- truncate
  -> [Cont]                                  -- ^ current chain
  -> HalfsM b r l m ([Cont], [Cont], Word64) -- ^ truncated chain, dirty conts,
                                             -- number of blocks freed
truncUnalloc dev bm start len conts = do
  let truncToZero = start + len == 0
  eIdx@(eContIdx, eBlkOff, _) <- decompStreamOffset (bdBlockSize dev) 
                                   (if truncToZero then 0 else start + len - 1)
  let 
    (retain, toFree) = genericSplitAt (eContIdx + 1) conts
    trm              = last retain 
    retain'          = init retain ++ dirtyConts
    keepBlkCnt       = if truncToZero then 0 else eBlkOff + 1
    allFreeBlks      = genericDrop keepBlkCnt (blockAddrs trm)
                       -- ^ The remaining blocks in the terminator
                       ++ concatMap blockAddrs toFree
                       -- ^ The remaining blocks in rest of chain
                       ++ map (unCR . address) toFree
                       -- ^ Block addrs for the cont blocks themselves
    numFreed         = genericLength allFreeBlks

    -- Currently, only the last Cont in the chain is considered dirty; we do not
    -- do any writes to any of the Conts that are detached from the chain &
    -- freed; this may have implications for fsck!
    dirtyConts =
      [
        -- The new terminator Cont, adjusted to discard the freed blocks
        -- and clear the continuation field
        trm { blockCount   = keepBlkCnt
            , blockAddrs   = genericTake keepBlkCnt (blockAddrs trm)
            , continuation = nilContRef
            }
      ]

  dbug ("truncUnalloc: keepBlkCnt  = " ++ show keepBlkCnt)  $ return ()
  dbug ("truncUnalloc: retain      = " ++ show retain)      $ return ()
  dbug ("truncUnalloc: toFree      = " ++ show toFree)      $ return ()
  dbug ("truncUnalloc: eIdx        = " ++ show eIdx)        $ return ()
  dbug ("truncUnalloc: retain'     = " ++ show retain')     $ return ()
  dbug ("truncUnalloc: freeNodes   = " ++ show toFree)      $ return ()
  dbug ("truncUnalloc: allFreeBlks = " ++ show allFreeBlks) $ return ()

  -- Freeing all of the blocks this way (as unit extents) is ugly and
  -- inefficient, but we need to be tracking BlockGroups (or
  -- reconstitute them here by digging for contiguous address
  -- subsequences in allFreeBlks) before we can do better.
    
  unless (null allFreeBlks) $ do
    lift $ BM.unallocBlocks bm $ BM.Discontig $ map (`BM.Extent` 1) allFreeBlks
    
  return (retain', dirtyConts, numFreed)

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

-- | Reads the contents of the given conts's ith block
readBlock :: (Monad m) =>
             BlockDevice m
          -> Cont
          -> Word64
          -> m ByteString
readBlock dev c i = do 
  assert (i < blockCount c) $ return ()
  bdReadBlock dev (blockAddrs c !! safeToInt i)

writeCont :: Monad m =>
             BlockDevice m -> Cont -> m ()
writeCont dev c = bdWriteBlock dev (unCR $ address c) (encode c)

writeInode :: (Monad m, Ord t, Serialize t, Show t) =>
              BlockDevice m -> Inode t -> m ()
writeInode dev n = bdWriteBlock dev (unIR $ inoAddress n) (encode n)

-- | Expands the given Cont into a Cont list containing itself followed by all
-- of its continuation inodes

-- NB/TODO: We need to optimize/fix this function. The worst case is, e.g.,
-- writing a small number of bytes at a low offset into a huge file (and hence a
-- long continuation chain): we read the entire chain when examination of the
-- stream from the start to end offsets would be sufficient.

expandConts :: HalfsCapable b t r l m =>
               BlockDevice m -> Cont -> HalfsM b r l m [Cont]
expandConts dev start@Cont{ continuation = cr }
  | cr == nilContRef = return [start]
  | otherwise        = {-# SCC "myExpandConts" #-} (start:) `fmap` (drefCont dev cr >>= expandConts dev)

drefCont :: HalfsCapable b t r l m =>
            BlockDevice m -> ContRef -> HalfsM b r l m Cont
drefCont dev (CR addr) =
  lift (bdReadBlock dev addr) >>= decodeCont (bdBlockSize dev)

drefInode :: HalfsCapable b t r l m => 
             BlockDevice m -> InodeRef -> HalfsM b r l m (Inode t)
drefInode dev (IR addr) = do 
  lift (bdReadBlock dev addr) >>= decodeInode (bdBlockSize dev) 

setChangeTime :: (Ord t, Serialize t) => t -> Inode t -> Inode t
setChangeTime t nd = nd{ inoChangeTime = t }

-- | Decompose the given absolute byte offset into an inode's data stream into
-- Cont index (i.e., 0-based index into the cont chain), a block offset within
-- that Cont, and a byte offset within that block.  
decompStreamOffset :: (Serialize t, Timed t m, Monad m, Show t) => 
                      Word64 -- ^ Block size, in bytes
                   -> Word64 -- ^ Offset into the data stream
                   -> HalfsM b r l m StreamIdx
decompStreamOffset blkSz streamOff = do
  -- Note that the first Cont in a Cont chain always gets embedded in an Inode,
  -- and thus has differing capacity than the rest of the Conts, which are of
  -- uniform size.
  (stContBytes, contBytes, _, _) <- getSizes blkSz
  let (contIdx, contByteIdx) =
        if streamOff >= stContBytes
        then fmapFst (+1) $ (streamOff - stContBytes) `divMod` contBytes
        else (0, streamOff)
      (blkOff, byteOff)      = contByteIdx `divMod` blkSz
  return (contIdx, blkOff, byteOff)

getStreamIdx :: HalfsCapable b t r l m =>
                Word64 -- block size in bytse
             -> Word64 -- file size in bytes
             -> Word64 -- start byte index
             -> HalfsM b r l m StreamIdx
getStreamIdx blkSz fileSz start = do
  when (start > fileSz) $ throwError $ HE_InvalidStreamIndex start
  decompStreamOffset blkSz start

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

magicContStr :: String
magicContStr = "!!erutcurts tnoC sflah a si sihT"

magicContBytes :: [Word8]
magicContBytes = assert (length magicContStr == 32) $
                   map (fromIntegral . ord) magicContStr

cmagic1, cmagic2, cmagic3, cmagic4 :: ByteString
cmagic1 = BS.pack $ take 8 $ drop  0 magicContBytes
cmagic2 = BS.pack $ take 8 $ drop  8 magicContBytes
cmagic3 = BS.pack $ take 8 $ drop 16 magicContBytes
cmagic4 = BS.pack $ take 8 $ drop 24 magicContBytes

--------------------------------------------------------------------------------
-- Typeclass instances

instance (Show t, Eq t, Ord t, Serialize t) => Serialize (Inode t) where
  put n = do
    putByteString $ magic1

    put           $ inoParent n           
    put           $ inoAddress n          
    putWord64be   $ inoFileSize n
    putWord64be   $ inoAllocBlocks n 
    put           $ inoFileType n
    put           $ inoMode n
    
    putByteString $ magic2        

    putWord64be   $ inoNumLinks n
    put           $ inoCreateTime n       
    put           $ inoModifyTime n       
    put           $ inoAccessTime n       
    put           $ inoChangeTime n       
    put           $ inoUser n             
    put           $ inoGroup n            

    putByteString $ magic3        

    put           $ inoCont n             

    -- NB: For Conts that are inside inodes, the Serialize instance for Cont
    -- relies on only 8 + iPadSize bytes beyond this point (the inode magic
    -- number and some padding).  If you change this, you'll need to update the
    -- related calculations in Serialize Cont's get function!
    putByteString magic4
    replicateM_ iPadSize $ putWord8 padSentinel

  get = do
    checkMagic magic1

    par   <- get
    addr  <- get
    fsz   <- getWord64be
    blks  <- getWord64be
    ftype <- get
    fmode <- get

    checkMagic magic2

    nlnks <- getWord64be
    ctm   <- get
    mtm   <- get
    atm   <- get
    chtm  <- get
    unless (ctm <= mtm && ctm <= atm) $
        fail "Inode: Incoherent modified / creation / access times."
    usr  <- get
    grp  <- get

    checkMagic magic3

    c <- get
    
    checkMagic magic4    
    padding <- replicateM iPadSize $ getWord8
    assert (all (== padSentinel) padding) $ return ()

    return Inode
      { inoParent       = par
      , inoAddress      = addr
      , inoFileSize     = fsz
      , inoAllocBlocks  = blks
      , inoFileType     = ftype
      , inoMode         = fmode
      , inoNumLinks     = nlnks
      , inoCreateTime   = ctm
      , inoModifyTime   = mtm
      , inoAccessTime   = atm
      , inoChangeTime   = chtm
      , inoUser         = usr
      , inoGroup        = grp
      , inoCont         = c
      }
   where
    checkMagic x = do
      magic <- getBytes 8
      unless (magic == x) $ fail "Invalid Inode: magic number mismatch"

instance Serialize Cont where
  put c = do
    unless (numBlocks <= numAddrs') $
      fail $ "Corrupted Cont structure put: too many blocks"
    putByteString $ cmagic1
    put           $ address c
    put           $ continuation c 
    putByteString $ cmagic2
    putWord64be   $ blockCount c
    putByteString $ cmagic3
    forM_ blocks put
    replicateM_ fillBlocks $ put nilInodeRef

    putByteString cmagic4
    replicateM_ cPadSize $ putWord8 padSentinel
    where
      blocks     = blockAddrs c
      numBlocks  = length blocks
      numAddrs'  = safeToInt $ numAddrs c
      fillBlocks = numAddrs' - numBlocks

  get = do
    checkMagic cmagic1
    addr <- get
    cont <- get
    checkMagic cmagic2
    blkCnt <- getWord64be
    checkMagic cmagic3

    -- Some calculations differ slightly based on whether or not this Cont is
    -- embedded in an inode; in particular, the Inode writes a terminating magic
    -- number after the end of the serialized Cont, so we must account for that
    -- when calculating the number of blocks to read below.
           
    let isEmbedded = addr == nilContRef
    numBlockBytes <- do
      remb <- fmap fromIntegral remaining
      let numTrailingBytes =
            if isEmbedded
            then 8 + fromIntegral cPadSize + 8 + fromIntegral iPadSize
                 -- cmagic4, padding, inode's magic4, inode's padding <EOS>
            else 8 + fromIntegral cPadSize
                 -- cmagic4, padding, <EOS>
      return (remb - numTrailingBytes)

    let (numBlocks, check) = numBlockBytes `divMod` refSize
    unless (check == 0) $ fail "Cont: Bad remaining byte count for block list."

    unless (not isEmbedded && numBlocks >= minContBlocks ||
            isEmbedded     && numBlocks >= minInodeBlocks) $ 
      fail "Cont: Not enough space left for minimum number of blocks."

    blks <- filter (/= 0) `fmap` replicateM (safeToInt numBlocks) get

    checkMagic cmagic4
    padding <- replicateM cPadSize $ getWord8
    assert (all (== padSentinel) padding) $ return ()

    let na = error $ "numAddrs has not been populated via Data.Serialize.get "
                  ++ "for Cont; did you forget to use the " 
                  ++ "Inode.decodeCont wrapper?"
    return Cont
           { address      = addr
           , continuation = cont
           , blockCount   = blkCnt
           , blockAddrs   = blks
           , numAddrs     = na
           }
   where
    checkMagic x = do
      magic <- getBytes 8
      unless (magic == x) $ fail "Invalid Cont: magic number mismatch"
