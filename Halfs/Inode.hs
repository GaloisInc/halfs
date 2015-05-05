{-# LANGUAGE GeneralizedNewtypeDeriving, ScopedTypeVariables, BangPatterns #-}
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
  , isNilIR
  , nilIR
  , readStream
  , writeStream
  -- * for internal use only!
  , atomicModifyInode
  , atomicReadInode
  , bsReplicate
  , drefInode
  , expandExts -- for use by fsck
  , fileStat_lckd
  , freeInode
  , withLockedInode
  , writeStream_lckd
  -- * for testing: ought not be used by actual clients of this module!
  , Inode(..)
  , Ext(..)
  , ExtRef(..)
  , bsDrop
  , bsTake
  , computeMinimalInodeSize
  , computeNumAddrs
  , computeNumInodeAddrsM
  , computeNumExtAddrsM
  , computeSizes
  , decodeExt
  , decodeInode
  , minimalExtSize
  , minInodeBlocks
  , minExtBlocks
  , nilER
  , safeToInt
  , truncSentinel
  )
 where

import Control.Exception
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Data.Char
import Data.List (genericDrop, genericLength, genericTake)
import Data.Serialize
import qualified Data.Serialize.Get as G
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

-- import System.IO.Unsafe (unsafePerformIO)
dbug :: String -> a -> a
-- dbug = seq . unsafePerformIO . putStrLn
dbug _ = id
dbugM :: Monad m => String -> m ()
--dbugM s = dbug s $ return ()
dbugM _ = return ()

type HalfsM b r l m a = HalfsT HalfsError (Maybe (HalfsState b r l m)) m a

--------------------------------------------------------------------------------
-- Inode/Ext constructors, geometry calculation, and helpful constructors

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
nilIR :: InodeRef
nilIR = IR 0

isNilIR :: InodeRef -> Bool
isNilIR = (==) nilIR

-- | The nil Ext reference.  With the current Word64 representation and
-- the block layout assumptions, block 0 is the superblock, and thus an
-- invalid Ext reference.
nilER :: ExtRef
nilER = ER 0

isNilER :: ExtRef -> Bool
isNilER = (==) nilER

-- | The sentinel byte written to partial blocks when doing truncating writes
truncSentinel :: Word8
truncSentinel = 0xBA

-- | The sentinel byte written to the padded region at the end of Inodes/Exts
padSentinel :: Word8
padSentinel = 0xAD

-- We semi-arbitrarily state that an Inode must be capable of maintaining a
-- minimum of 35 block addresses in its embedded Ext while the Ext must be
-- capable of maintaining 57 block addresses.  These values, together with
-- specific padding values for inodes and exts (4 and 0, respectively), give us
-- a minimum inode AND ext size of 512 bytes each (in the IO monad variant,
-- which uses the our Serialize instance for the UTCTime when writing the time
-- fields).
--
-- These can be adjusted as needed according to inode metadata sizes, but it's
-- very important that (computeMinimalInodeSize =<< getTime) and minimalExtSize yield
-- the same value!

-- | The size, in bytes, of the padding region at the end of Inodes
iPadSize :: Int
iPadSize = 4

-- | The size, in bytes, of the padding region at the end of Exts
cPadSize :: Int
cPadSize = 0

minInodeBlocks :: Word64
minInodeBlocks = 35

minExtBlocks :: Word64
minExtBlocks = 57

-- | The structure of an Inode. Pretty standard, except that we use the Ext
-- structure (the first of which is embedded in the inode) to hold block
-- references and use its ext field to allow multiple runs of block
-- addresses.
data Inode t = Inode
  { inoParent      :: InodeRef         -- ^ block addr of parent directory
                                       -- inode: This is nilIR for the
                                       -- root directory inode

  , inoLastExt     :: (ExtRef, Word64) -- ^ The last-accessed ER and its ext
                                       -- idx.  For the "faster end-of-stream
                                       -- access" hack.
    -- begin fstat metadata
  , inoAddress     :: InodeRef -- ^ block addr of this inode
  , inoFileSize    :: Word64   -- ^ in bytes
  , inoAllocBlocks :: Word64   -- ^ number of blocks allocated to this inode
                               --   (includes its own allocated block, blocks
                               --   allocated for Exts, and and all blocks in
                               --   the ext chain itself)
  , inoFileType    :: FileType
  , inoMode        :: FileMode
  , inoNumLinks    :: Word64   -- ^ number of hardlinks to this inode
  , inoCreateTime  :: t        -- ^ time of creation
  , inoModifyTime  :: t        -- ^ time of last data modification
  , inoAccessTime  :: t        -- ^ time of last data access
  , inoChangeTime  :: t        -- ^ time of last change to inode data
  , inoUser        :: UserID   -- ^ userid of inode's owner
  , inoGroup       :: GroupID  -- ^ groupid of inode's owner
  -- end fstat metadata

  , inoExt         :: Ext   -- The "embedded" inode extension ("ext")
  }
  deriving (Show, Eq)

-- An "Inode extension" datatype
data Ext = Ext
  { address      :: ExtRef   -- ^ Address of this Ext (nilER for an
                             --   inode's embedded Ext)
  , nextExt      :: ExtRef  -- ^ Next Ext in the chain; nilER terminates
  , blockCount   :: Word64
  , blockAddrs   :: [Word64] -- ^ references to blocks governed by this Ext

  -- Fields below here are not persisted, and are populated via decodeExt

  , numAddrs     :: Word64   -- ^ Maximum number of blocks addressable by *this*
                             --   Ext.  NB: Does not include blocks further down
                             --   the chain.
  }
  deriving (Show, Eq)

-- | Size of a minimal inode structure when serialized, in bytes.  This will
-- vary based on the space required for type t when serialized.  Note that
-- minimal inode structure always contains minInodeBlocks InodeRefs in
-- its blocks region.
--
-- You can check this value interactively in ghci by doing, e.g.
-- computeMinimalInodeSize =<< (getTime :: IO UTCTime)
computeMinimalInodeSize :: (Monad m, Ord t, Serialize t, Show t) =>
                           t -> m Word64
computeMinimalInodeSize t = do
  return $ fromIntegral $ BS.length $ encode $
    let e = emptyInode minInodeBlocks t RegularFile (FileMode [] [] [])
                       nilIR nilIR rootUser rootGroup
        c = inoExt e
    in e{ inoExt = c{ blockAddrs = replicate (safeToInt minInodeBlocks) 0 } }

-- | The size of a minimal Ext structure when serialized, in bytes.
minimalExtSize :: Monad m => m (Word64)
minimalExtSize = return $ fromIntegral $ BS.length $ encode $
  (emptyExt minExtBlocks nilER){
    blockAddrs = replicate (safeToInt minExtBlocks) 0
  }

-- | Computes the number of block addresses storable by an inode/ext
computeNumAddrs :: Monad m =>
                   Word64 -- ^ block size, in bytes
                -> Word64 -- ^ minimum number of blocks for inode/ext
                -> Word64 -- ^ minimum inode/ext total size, in bytes
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
  computeNumAddrs blkSz minInodeBlocks =<< computeMinimalInodeSize =<< getTime

computeNumExtAddrsM :: (Serialize t, Timed t m) =>
                       Word64 -> m Word64
computeNumExtAddrsM blkSz = do
  minSize <- minimalExtSize
  computeNumAddrs blkSz minExtBlocks minSize

computeSizes :: (Serialize t, Timed t m, Show t) =>
                Word64
             -> m ( Word64 -- #inode bytes
                  , Word64 -- #ext bytes
                  , Word64 -- #inode addrs
                  , Word64 -- #ext addrs
                  )
computeSizes blkSz = do
  startExtAddrs <- computeNumInodeAddrsM blkSz
  extAddrs      <- computeNumExtAddrsM  blkSz
  return (startExtAddrs * blkSz, extAddrs * blkSz, startExtAddrs, extAddrs)

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
  now       <- getTime
  minSize   <- computeMinimalInodeSize =<< return now

  minimalExtSize >>= (`assert` return ()) . (==) minSize

  nAddrs    <- computeNumAddrs (bdBlockSize bd) minInodeBlocks minSize
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
  , inoLastExt      = (nilER, 0)
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
  , inoExt          = emptyExt nAddrs nilER
  }

buildEmptyExt :: (Serialize t, Timed t m) =>
                 BlockDevice m -- ^ The block device
              -> ExtRef        -- ^ This ext's block address
              -> m Ext
buildEmptyExt bd me = do
  minSize <- minimalExtSize
  nAddrs  <- computeNumAddrs (bdBlockSize bd) minExtBlocks minSize
  return $ emptyExt nAddrs me

emptyExt :: Word64  -- ^ number of block addresses
         -> ExtRef -- ^ block addr for this ext
         -> Ext
emptyExt nAddrs me =
  Ext
  { address      = me
  , nextExt      = nilER
  , blockCount   = 0
  , blockAddrs   = []
  , numAddrs     = nAddrs
  }


--------------------------------------------------------------------------------
-- Inode stream functions

-- | Provides a stream over the bytes governed by a given Inode and its
-- extensions (exts).  This function performs a write to update inode metadata
-- (e.g., access time).
readStream :: HalfsCapable b t r l m =>
              InodeRef                  -- ^ Starting inode reference
           -> Word64                    -- ^ Starting stream (byte) offset
           -> Maybe Word64              -- ^ Stream length (Nothing => read
                                        --   until end of stream, including
                                        --   entire last block)
           -> HalfsM b r l m ByteString -- ^ Stream contents
readStream startIR start mlen = withLockedInode startIR $ do
  -- ====================== Begin inode critical section ======================
  dev        <- hasks hsBlockDev
  startInode <- drefInode startIR
  let bs        = bdBlockSize dev
      readB c b = lift $ readBlock dev c b
      fileSz    = inoFileSize startInode
      gsi       = getStreamIdx (bdBlockSize dev) fileSz

  if 0 == blockCount (inoExt startInode)
   then return BS.empty
   else dbug ("==== readStream begin ===") $ do
     (sExtI, sBlkOff, sByteOff) <- gsi start
     sExt                       <- findExt startInode sExtI
     (eExtI, _, _)              <- gsi $ case mlen of
                                            Nothing  -> fileSz - 1
                                            Just len -> start + len - 1

     dbugM ("start                       = " ++ show start)
     dbugM ("(sExtI, sBlkOff, sByteOff) = " ++ show (sExtI, sBlkOff, sByteOff))
     dbugM ("eExtI                      = " ++ show eExtI)

     rslt <- case mlen of
       Just len | len == 0 -> return BS.empty
       _                   -> do
         assert (maybe True (> 0) mlen) $ return ()

         -- 'hdr' is the (possibly partial) first block
         hdr <- bsDrop sByteOff `fmap` readB sExt sBlkOff

         -- 'rest' is the list of block-sized bytestrings containing the
         -- requested content from blocks in subsequent conts, accounting for
         -- the (Maybe) maximum length requested.
         rest <- do
           let hdrLen      = fromIntegral (BS.length hdr)
               totalToRead = maybe (fileSz - start) id mlen
               --
               howManyBlks ext bsf blkOff =
                 let bc = blockCount ext - blkOff
                 in
                 maybe bc (\len -> min bc ((len - bsf) `divCeil` bs)) mlen
               --
               readExt (_, [], _) = error "The impossible happened"
               readExt ((cExt, cExtI), blkOff:boffs, bytesSoFar)
                 | cExtI > eExtI =
                     assert (bytesSoFar >= totalToRead) $ return Nothing
                 | bytesSoFar >= totalToRead =
                     return Nothing
                 | otherwise = do
                     let remBlks = howManyBlks cExt bytesSoFar blkOff
                         range   = if remBlks > 0
                                    then [blkOff .. blkOff + remBlks - 1]
                                    else []

                     theData <- BS.concat `fmap` mapM (readB cExt) range
                     assert (fromIntegral (BS.length theData) == remBlks * bs) $ return ()

                     let rslt c = return $ Just $
                                  ( theData -- accumulated by unfoldr
                                  , ( (c, cExtI+1)
                                    , boffs
                                    , bytesSoFar + remBlks * bs
                                    )
                                  )
                     if isNilER (nextExt cExt)
                      then rslt (error "Ext DNE and expected termination!")
                      else rslt =<< drefExt (nextExt cExt)

           -- ==> Bulk reading starts here <==
           if (sBlkOff + 1 < blockCount sExt || sExtI < eExtI)
            then unfoldrM readExt ((sExt, sExtI), (sBlkOff+1):repeat 0, hdrLen)
            else return []

         return $ bsTake (maybe (fileSz - start) id mlen) $
           hdr `BS.append` BS.concat rest

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
  withLockedInode startIR $ writeStream_lckd startIR start trunc bytes

writeStream_lckd :: HalfsCapable b t r l m =>
                    InodeRef   -- ^ Starting inode ref
                 -> Word64     -- ^ Starting stream (byte) offset
                 -> Bool       -- ^ Truncating write?
                 -> ByteString -- ^ Data to write
                 -> HalfsM b r l m ()
writeStream_lckd _ _ False bytes | 0 == BS.length bytes = return ()
writeStream_lckd startIR start trunc bytes              = do
  -- ====================== Begin inode critical section ======================

  -- NB: This implementation currently 'flattens' Contig/Discontig block groups
  -- from the BlockMap allocator (see allocFill and truncUnalloc below), which
  -- will force us to treat them as Discontig when we unallocate, which is more
  -- expensive.  We may want to have the Exts hold onto these block groups
  -- directly and split/merge them as needed to reduce the number of
  -- unallocation actions required, but we leave this as a TODO for now.

  startInode@Inode{ inoLastExt = lcInfo } <- drefInode startIR
  dev                                     <- hasks hsBlockDev

  let bs           = bdBlockSize dev
      len          = fromIntegral $ BS.length bytes
      fileSz       = inoFileSize startInode
      newFileSz    = if trunc then start + len else max (start + len) fileSz
      fszRndBlk    = (fileSz `divCeil` bs) * bs
      availBlks c  = numAddrs c - blockCount c
      bytesToAlloc = if newFileSz > fszRndBlk then newFileSz - fszRndBlk else 0
      blksToAlloc  = bytesToAlloc `divCeil` bs

  (_, _, _, apc)                   <- hasks hsSizes
  sIdx@(sExtI, sBlkOff, sByteOff)  <- getStreamIdx bs fileSz start
  sExt                            <- findExt startInode sExtI
  lastExt                         <- getLastExt Nothing sExt
                                      -- TODO: Track a ptr to this?  Traversing
                                      -- the allocated region is yucky.
  let extsToAlloc = (blksToAlloc - availBlks lastExt) `divCeil` apc

  ------------------------------------------------------------------------------
  -- Debugging miscellany

  dbugM ("\nwriteStream: " ++ show (sExtI, sBlkOff, sByteOff)
        ++ " (start=" ++ show start ++ ")"
        ++ " len = " ++ show len ++ ", trunc = " ++ show trunc
        ++ ", fileSz/newFileSz = " ++ show fileSz ++ "/" ++ show newFileSz
        ++ ", toAlloc(exts/blks/bytes) = " ++ show extsToAlloc ++ "/"
        ++ show blksToAlloc ++ "/" ++ show bytesToAlloc)
--   dbug ("inoLastExt startInode = " ++ show (lcInfo) )            $ return ()
--   dbug ("inoExt startInode     = " ++ show (inoExt startInode)) $ return ()
--   dbug ("Exts on entry, from inode ext:") $ return ()
--   dumpExts (inoExt startInode)

  ------------------------------------------------------------------------------
  -- Allocation:

  -- Allocate if needed and obtain (1) the post-alloc start ext and (2)
  -- possibly a dirty ext to write back into the inode (ie, when its
  -- nextExt field is modified as a result of allocation -- all other
  -- modified exts are written by allocFill, but the inode write is the last
  -- thing we do, so we defer the update).

  (sExt', minodeExt) <- do
    if blksToAlloc == 0
     then return (sExt, Nothing)
     else do
       lastExt' <- allocFill availBlks blksToAlloc extsToAlloc lastExt
       let st  = if address lastExt' == address sExt then lastExt' else sExt
                 -- Our starting location remains the same, but in case of an
                 -- update of the start ext, we can use lastExt' instead of
                 -- re-reading.
           lci = snd lcInfo
       st' <- if lci < sExtI
               then getLastExt (Just $ sExtI - lci + 1) st
                    -- NB: We need to start ahead of st, but couldn't adjust
                    -- until after we allocated. This is to catch a corner case
                    -- where the "start" ext coming into writeStream_lckd
                    -- refers to a ext that hasn't been allocated yet.
               else return st
       return (st', if isEmbedded st then Just st else Nothing)

--   dbug ("sExt' = " ++ show sExt') $ return ()
--   dbug ("minodeExt = " ++ show minodeExt) $ return ()
--   dbug ("Exts immediately after alloc, from sExt'") $ return ()
--   dumpExts (sExt')

  assert (sBlkOff < blockCount sExt') $ return ()

  ------------------------------------------------------------------------------
  -- Truncation

  -- Truncate if needed and obtain (1) the new start ext at which to start
  -- writing data and (2) possibly a dirty ext to write back into the inode

  (sExt'', numBlksFreed, minodeExt') <-
    if trunc && bytesToAlloc == 0
     then do
       (ext, nbf) <- truncUnalloc start len (sExt', sExtI)
       return ( ext
              , nbf
              , if isEmbedded ext
                 then case minodeExt of
                        Nothing -> Just ext
                        Just _  -> -- This "can't happen" ...
                                  error $ "Internal: dirty inode ext from  "
                                          ++ "both allocFill & truncUnalloc!?"
                 else Nothing
              )
     else return (sExt', 0, minodeExt)

  assert (blksToAlloc + extsToAlloc == 0 || numBlksFreed == 0) $ return ()

  ------------------------------------------------------------------------------
  -- Data streaming

  (eExtI, _, _) <- decomp (bdBlockSize dev) (bytesToEnd start len)
  eExt          <- getLastExt (Just $ (eExtI - sExtI) + 1) sExt''
  when (len > 0) $ writeInodeData (sIdx, sExt'') eExtI trunc bytes

  --------------------------------------------------------------------------------
  -- Inode metadata adjustments & persist

  now <- getTime
  lift $ writeInode dev $
    startInode
    { inoLastExt    = if eExtI == sExtI
                        then (address sExt'', sExtI)
                        else (address eExt, eExtI)
    , inoFileSize    = newFileSz
    , inoAllocBlocks = inoAllocBlocks startInode
                       + blksToAlloc
                       + extsToAlloc
                       - numBlksFreed
    , inoAccessTime  = now
    , inoModifyTime  = now
    , inoChangeTime  = now
    , inoExt         = maybe (inoExt startInode) id minodeExt'
    }
-- ======================= End inode critical section =======================


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
    inode  <- drefInode inr
    now    <- getTime
    inode' <- setChangeTime now `fmap` f inode
    lift $ writeInode dev inode'

atomicReadInode :: HalfsCapable b t r l m =>
                    InodeRef
                -> (Inode t -> a)
                -> HalfsM b r l m a
atomicReadInode inr f =
  withLockedInode inr $ f `fmap` drefInode inr

fileStat :: HalfsCapable b t r l m =>
            InodeRef
         -> HalfsM b r l m (FileStat t)
fileStat inr =
  withLockedInode inr $ fileStat_lckd inr

fileStat_lckd :: HalfsCapable b t r l m =>
                 InodeRef
              -> HalfsM b r l m (FileStat t)
fileStat_lckd inr = do
  inode <- drefInode inr
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
-- Inode/Ext stream helper & utility functions

isEmbedded :: Ext -> Bool
isEmbedded = isNilER . address

freeInode :: HalfsCapable b t r l m =>
             InodeRef -- ^ reference to the inode to remove
          -> HalfsM b r l m ()
freeInode inr@(IR addr) =
  withLockedInode inr $ do
    bm    <- hasks hsBlockMap
    start <- inoExt `fmap` drefInode inr
    freeBlocks bm (blockAddrs start)
    _numFreed :: Word64 <- freeExts bm start
    BM.unalloc1 bm addr

freeBlocks :: HalfsCapable b t r l m =>
              BlockMap b r l -> [Word64] -> HalfsM b r l m ()
freeBlocks _ []     = return ()
freeBlocks bm addrs =
  lift $ BM.unallocBlocks bm $ BM.Discontig $ map (`BM.Extent` 1) addrs
  -- NB: Freeing all of the blocks this way (as unit extents) is ugly and
  -- inefficient, but we need to be tracking BlockGroups (or reconstitute them
  -- here by digging for contiguous address subsequences in addrs) before we can
  -- do better.

-- | Frees all exts after the given ext, returning the number of blocks freed.
freeExts :: (HalfsCapable b t r l m, Num a) =>
             BlockMap b r l -> Ext -> HalfsM b r l m a
freeExts bm Ext{ nextExt = cr }
  | isNilER cr = return $ fromInteger 0
  | otherwise  = drefExt cr >>= extFoldM freeExt (fromInteger 0)
  where
    freeExt !acc c =
      freeBlocks bm toFree >> return (acc + genericLength toFree)
        where toFree = unER (address c) : blockAddrs c

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
               ByteString
            -> HalfsM b r l m (Inode t)
decodeInode bs = do
  (_, _, numAddrs', _) <- hasks hsSizes
  case decode bs of
    Left s  -> throwError $ HE_DecodeFail_Inode s
    Right n -> do
      return n{ inoExt = (inoExt n){ numAddrs = numAddrs' } }

-- | A wrapper around Data.Serialize.decode that populates transient fields.  We
-- do this to avoid occupying valuable on-disk Ext space where possible.  Bare
-- applications of 'decode' should not occur when deserializing Exts!
decodeExt :: HalfsCapable b t r l m =>
              Word64
           -> ByteString
           -> HalfsM b r l m Ext
decodeExt blkSz bs = do
  numAddrs' <- computeNumExtAddrsM blkSz
  case decode bs of
    Left s  -> throwError $ HE_DecodeFail_Ext s
    Right c -> return c{ numAddrs = numAddrs' }

-- | Obtain the ext with the given ext index in the ext chain.
-- Currently traverses Exts from either the inode's embedded ext or
-- from the (saved) ext from the last operation, whichever is
-- closest.
findExt :: HalfsCapable b t r l m =>
            Inode t -> Word64 -> HalfsM b r l m Ext
findExt Inode{ inoLastExt = (ler, lci), inoExt = defExt } sci
  | isNilER ler || lci > sci = getLastExt (Just $ sci + 1) defExt
  | otherwise = getLastExt (Just $ sci - lci + 1) =<< drefExt ler

-- | Allocate the given number of Exts and blocks, and fill blocks into the
-- ext chain starting at the given ext.  Persists dirty exts and yields a new
-- start ext to use.
allocFill :: HalfsCapable b t r l m =>
             (Ext -> Word64)    -- ^ Available blocks function
          -> Word64              -- ^ Number of blocks to allocate
          -> Word64              -- ^ Number of exts to allocate
          -> Ext                -- ^ Last allocated ext
          -> HalfsM b r l m Ext -- ^ Updated last ext
allocFill _     0           _            eExt = return eExt
allocFill avail blksToAlloc extsToAlloc eExt = do
  dev      <- hasks hsBlockDev
  bm       <- hasks hsBlockMap
  newExts <- allocExts dev bm
  blks     <- allocBlocks bm

  -- Fill nextExt fields to form the region that we'll fill with the newly
  -- allocated blocks (i.e., starting at the end of the already-allocated region
  -- from the start ext, but including the newly allocated exts as well).

  let (_, region) = foldr (\c (er, !acc) ->
                             ( address c
                             , c{ nextExt = er } : acc
                             )
                          )
                          (nilER, [])
                          (eExt : newExts)

  -- "Spill" the allocated blocks into the empty region
  let (blks', k)               = foldl fillBlks (blks, id) region
      dirtyExts@(eExt':_)    = k []
      fillBlks (remBlks, k') c =
        let cnt    = min (safeToInt $ avail c) (length remBlks)
            c'     = c { blockCount = blockCount c + fromIntegral cnt
                       , blockAddrs = blockAddrs c ++ take cnt remBlks
                       }
        in
          (drop cnt remBlks, k' . (c':))

  assert (null blks') $ return ()

  forM_ (dirtyExts)  $ \c -> unless (isEmbedded c) $ lift $ writeExt dev c
  return eExt'
  where
    allocBlocks bm = do
      -- currently "flattens" BlockGroup; see comment in writeStream
      mbg <- lift $ BM.allocBlocks bm blksToAlloc
      case mbg of
        Nothing -> throwError HE_AllocFailed
        Just bg -> return $ BM.blkRangeBG bg
    --
    allocExts dev bm =
      if 0 == extsToAlloc
      then return []
      else do
        -- TODO: Unalloc partial allocs on HE_AllocFailed?
        mexts <- fmap sequence $ replicateM (safeToInt extsToAlloc) $ do
          mcr <- fmap ER `fmap` BM.alloc1 bm
          case mcr of
            Nothing -> return Nothing
            Just cr -> Just `fmap` lift (buildEmptyExt dev cr)
        maybe (throwError HE_AllocFailed) (return) mexts

-- | Truncates the stream at the given a stream index and length offset, and
-- unallocates all resources in the corresponding free region, yielding a new
-- Ext for the truncated chain.
truncUnalloc ::
  HalfsCapable b t r l m =>
     Word64         -- ^ Starting stream byte index
  -> Word64         -- ^ Length from start at which to truncate
  -> (Ext, Word64) -- ^ Start ext of chain to truncate, start ext idx
  -> HalfsM b r l m (Ext, Word64)
     -- ^ new start ext, number of blocks freed
truncUnalloc start len (stExt, sExtI) = do
  dev <- hasks hsBlockDev
  bm  <- hasks hsBlockMap

  (eExtI, eBlkOff, _) <- decomp (bdBlockSize dev) (bytesToEnd start len)
  assert (eExtI >= sExtI) $ return ()

  -- Get the (new) end of the ext chain. Retain all exts in [sExtI, eExtI].
  eExt <- getLastExt (Just $ eExtI - sExtI + 1) stExt

  let keepBlkCnt  = if start + len == 0 then 0 else eBlkOff + 1
      endExtRem  = genericDrop keepBlkCnt (blockAddrs eExt)

  freeBlocks bm endExtRem
  numFreed <- (+ (genericLength endExtRem)) `fmap` freeExts bm eExt

  -- Currently, only eExt is considered dirty; we do *not* do any writes to any
  -- of the Exts that are detached from the chain & freed (we just toss them
  -- out); this may have implications for fsck.
  let dirtyExts@(firstDirty:_) =
        [
          -- eExt, adjusted to discard the freed blocks and clear the
          -- nextExt field.
          eExt { blockCount  = keepBlkCnt
                , blockAddrs = genericTake keepBlkCnt (blockAddrs eExt)
                , nextExt    = nilER
                }
        ]
      stExt' = if sExtI == eExtI then firstDirty else stExt

  forM_ (dirtyExts) $ \c -> unless (isEmbedded c) $ lift $ writeExt dev c
  return (stExt', numFreed)

-- | Write the given bytes to the already-allocated/truncated inode data stream
-- starting at the given start indices (ext/blk/byte offsets) and ending when
-- we have traversed up (and including) to the end ext index.  Assumes the
-- inode lock is held.
writeInodeData :: HalfsCapable b t r l m =>
                  (StreamIdx, Ext)
               -> Word64
               -> Bool
               -> ByteString
               -> HalfsM b r l m ()
writeInodeData ((sExtI, sBlkOff, sByteOff), sExt) eExtI trunc bytes = do
  dev  <- hasks hsBlockDev
  sBlk <- lift $ readBlock dev sExt sBlkOff
  let bs      = bdBlockSize dev
      toWrite =
        -- The first block-sized chunk to write is the region in the start block
        -- prior to the start byte offset (header), followed by the first bytes
        -- of the data.  The trailer is nonempty and must be included when
        -- BS.length bytes < bs.  We adjust the input bytestring by this
        -- first-block padding below.
        let (sData, bytes') = bsSplitAt (bs - sByteOff) bytes
            header          = bsTake sByteOff sBlk
            trailLen        = sByteOff + fromIntegral (BS.length sData)
            trailer         = if trunc
                               then bsReplicate (bs - trailLen) truncSentinel
                               else bsDrop trailLen sBlk
            fstBlk          = header `BS.append` sData `BS.append` trailer
        in assert (fromIntegral (BS.length fstBlk) == bs) $
             fstBlk `BS.append` bytes'

  -- The unfoldr seed is: current ext/idx, a block offset "supply", and the
  -- data that remains to be written.
  unfoldrM_ (fillExt dev) ((sExt, sExtI), sBlkOff : repeat 0,  toWrite)
  where
    fillExt _ (_, [], _) = error "The impossible happened"
    fillExt dev ((cExt, cExtI), blkOff:boffs, toWrite)
      | cExtI > eExtI || BS.null toWrite = return Nothing
      | otherwise = do
          let blkAddrs  = genericDrop blkOff (blockAddrs cExt)
              split crs = let (cs, rems) = unzip crs in (cs, last rems)
              gbc       = lift . getBlockContents dev trunc

          (chunks, remBytes) <- split `fmap` unfoldrM gbc (toWrite, blkAddrs)

          assert (let lc = length chunks; lb = length blkAddrs
                  in lc == lb || (BS.length remBytes == 0 && lc < lb)) $ return ()

          mapM_ (lift . uncurry (bdWriteBlock dev)) (blkAddrs `zip` chunks)

          if isNilER (nextExt cExt)
           then assert (BS.null remBytes) $ return Nothing
           else do
             nextExt' <- drefExt (nextExt cExt)
             return $ Just $ ((), ((nextExt', cExtI+1), boffs, remBytes))


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
  -> m (Maybe ((ByteString, ByteString), (ByteString, [Word64])))
  -- ^ When unfolding, the collected result type here of (ByteString,
  -- ByteString) is (chunk, remaining data), in case the entire input bytestring
  -- is not consumed.  The seed value is (bytestring for all data, block
  -- addresses for each chunk).
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
     return $ Just ((rslt, remBytes), (remBytes, blkAddrs))
   else do
     -- Full block; nothing to see here
     return $ Just ((newBlkData, remBytes), (remBytes, blkAddrs))

-- | Reads the contents of the given exts's ith block
readBlock :: (Monad m) =>
             BlockDevice m
          -> Ext
          -> Word64
          -> m ByteString
readBlock dev c i = do
  assert (i < blockCount c) $ return ()
  bdReadBlock dev (blockAddrs c !! safeToInt i)

writeExt :: Monad m =>
             BlockDevice m -> Ext -> m ()
writeExt dev c =
  dbug ("  ==> Writing ext: " ++ show c ) $
  bdWriteBlock dev (unER $ address c) (encode c)

writeInode :: (Monad m, Ord t, Serialize t, Show t) =>
              BlockDevice m -> Inode t -> m ()
writeInode dev n = bdWriteBlock dev (unIR $ inoAddress n) (encode n)

-- | Expands the given Ext into a Ext list containing itself followed by zero
-- or more Exts; can be bounded by a positive nonzero value to only retrieve
-- (up to) the given number of exts.
expandExts :: HalfsCapable b t r l m =>
               Maybe Word64 -> Ext -> HalfsM b r l m [Ext]
expandExts (Just bnd) start@Ext{ nextExt = cr }
  | bnd == 0                      = throwError HE_InvalidExtIdx
  | isNilER cr || bnd == 1 = return [start]
  | otherwise                     = do
      (start:) `fmap` (drefExt cr >>= expandExts (Just (bnd - 1)))
expandExts Nothing start@Ext{ nextExt = cr }
  | isNilER cr = return [start]
  | otherwise        = do
      (start:) `fmap` (drefExt cr >>= expandExts Nothing)

getLastExt :: HalfsCapable b t r l m =>
               Maybe Word64 -> Ext -> HalfsM b r l m Ext
getLastExt mbnd c = last `fmap` expandExts mbnd c

extFoldM :: HalfsCapable b t r l m =>
             (a -> Ext -> HalfsM b r l m a) -> a -> Ext -> HalfsM b r l m a
extFoldM f a c@Ext{ nextExt = cr }
  | isNilER cr = f a c
  | otherwise  = f a c >>= \fac -> drefExt cr >>= extFoldM f fac

extMapM :: HalfsCapable b t r l m =>
            (Ext -> HalfsM b r l m a) -> Ext -> HalfsM b r l m [a]
extMapM f c@Ext{ nextExt = cr }
  | isNilER cr = liftM (:[]) (f c)
  | otherwise  = liftM2 (:) (f c) (drefExt cr >>= extMapM f)

drefExt :: HalfsCapable b t r l m =>
            ExtRef -> HalfsM b r l m Ext
drefExt cr@(ER addr) | isNilER cr = throwError HE_InvalidExtIdx
                      | otherwise  = do
      dev <- hasks hsBlockDev
      lift (bdReadBlock dev addr) >>= decodeExt (bdBlockSize dev)

drefInode :: HalfsCapable b t r l m =>
             InodeRef -> HalfsM b r l m (Inode t)
drefInode (IR addr) = do
  dev <- hasks hsBlockDev
  lift (bdReadBlock dev addr) >>= decodeInode

setChangeTime :: (Ord t, Serialize t) => t -> Inode t -> Inode t
setChangeTime t nd = nd{ inoChangeTime = t }

-- | Decompose the given absolute byte offset into an inode's data stream into
-- Ext index (i.e., 0-based index into the ext chain), a block offset within
-- that Ext, and a byte offset within that block.
decomp :: (Serialize t, Timed t m, Monad m, Show t) =>
          Word64 -- ^ Block size, in bytes
       -> Word64 -- ^ Offset into the data stream
       -> HalfsM b r l m StreamIdx
decomp blkSz streamOff = do
  -- Note that the first Ext in a Ext chain always gets embedded in an Inode,
  -- and thus has differing capacity than the rest of the Exts, which are of
  -- uniform size.
  (stExtBytes, extBytes, _, _) <- hasks hsSizes
  let (extIdx, extByteIdx) =
        if streamOff >= stExtBytes
        then fmapFst (+1) $ (streamOff - stExtBytes) `divMod` extBytes
        else (0, streamOff)
      (blkOff, byteOff)      = extByteIdx `divMod` blkSz
  return (extIdx, blkOff, byteOff)

getStreamIdx :: HalfsCapable b t r l m =>
                Word64 -- block size in bytse
             -> Word64 -- file size in bytes
             -> Word64 -- start byte index
             -> HalfsM b r l m StreamIdx
getStreamIdx blkSz fileSz start = do
  when (start > fileSz) $ throwError $ HE_InvalidStreamIndex start
  decomp blkSz start

bytesToEnd :: Word64 -> Word64 -> Word64
bytesToEnd start len
  | start + len == 0 = 0
  | otherwise        = max (start + len - 1) start

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

magicExtStr :: String
magicExtStr = "!!erutcurts tnoC sflah a si sihT"

magicExtBytes :: [Word8]
magicExtBytes = assert (length magicExtStr == 32) $
                   map (fromIntegral . ord) magicExtStr

cmagic1, cmagic2, cmagic3, cmagic4 :: ByteString
cmagic1 = BS.pack $ take 8 $ drop  0 magicExtBytes
cmagic2 = BS.pack $ take 8 $ drop  8 magicExtBytes
cmagic3 = BS.pack $ take 8 $ drop 16 magicExtBytes
cmagic4 = BS.pack $ take 8 $ drop 24 magicExtBytes

--------------------------------------------------------------------------------
-- Typeclass instances

instance (Show t, Eq t, Ord t, Serialize t) => Serialize (Inode t) where
  put n = do
    putByteString $ magic1

    put           $ inoParent n
    put           $ inoLastExt n
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

    put           $ inoExt n

    -- NB: For Exts that are inside inodes, the Serialize instance for Ext
    -- relies on only 8 + iPadSize bytes beyond this point (the inode magic
    -- number and some padding).  If you change this, you'll need to update the
    -- related calculations in Serialize Ext's get function!
    putByteString magic4
    replicateM_ iPadSize $ putWord8 padSentinel

  get = do
    checkMagic magic1

    par   <- get
    lcr   <- get
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
      { inoParent      = par
      , inoLastExt    = lcr
      , inoAddress     = addr
      , inoFileSize    = fsz
      , inoAllocBlocks = blks
      , inoFileType    = ftype
      , inoMode        = fmode
      , inoNumLinks    = nlnks
      , inoCreateTime  = ctm
      , inoModifyTime  = mtm
      , inoAccessTime  = atm
      , inoChangeTime  = chtm
      , inoUser        = usr
      , inoGroup       = grp
      , inoExt        = c
      }
   where
    checkMagic x = do
      magic <- getBytes 8
      unless (magic == x) $ fail "Invalid Inode: magic number mismatch"

instance Serialize Ext where
  put c = do
    unless (numBlocks <= numAddrs') $
      fail $ "Corrupted Ext structure put: too many blocks"

    -- dbugM $ "Ext.put: numBlocks = " ++ show numBlocks
    -- dbugM $ "Ext.put: blocks = " ++ show blocks
    -- dbugM $ "Ext.put: fillBlocks = " ++ show fillBlocks

    putByteString $ cmagic1
    put           $ address c
    put           $ nextExt c
    putByteString $ cmagic2
    putWord64be   $ blockCount c
    putByteString $ cmagic3
    forM_ blocks put
    replicateM_ fillBlocks $ put nilIR

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
    dbugM $ "decodeExt: addr = " ++ show addr
    ext <- get
    dbugM $ "decodeExt: ext = " ++ show ext
    checkMagic cmagic2
    blkCnt <- getWord64be
    dbugM $ "decodeExt: blkCnt = " ++ show blkCnt
    checkMagic cmagic3

    -- Some calculations differ slightly based on whether or not this Ext is
    -- embedded in an inode; in particular, the Inode writes a terminating magic
    -- number after the end of the serialized Ext, so we must account for that
    -- when calculating the number of blocks to read below.

    let isEmbeddedExt = addr == nilER
    dbugM $ "decodeExt: isEmbeddedExt = " ++ show isEmbeddedExt
    numBlockBytes <- do
      remb <- fmap fromIntegral G.remaining
      dbugM $ "decodeExt: remb = " ++ show remb
      dbugM $ "--"
      let numTrailingBytes =
            if isEmbeddedExt
            then 8 + fromIntegral cPadSize + 8 + fromIntegral iPadSize
                 -- cmagic4, padding, inode's magic4, inode's padding <EOS>
            else 8 + fromIntegral cPadSize
                 -- cmagic4, padding, <EOS>
      dbugM $ "decodeExt: numTrailingBytes = " ++ show numTrailingBytes
      return (remb - numTrailingBytes)
    dbugM $ "decodeExt: numBlockBytes = " ++ show numBlockBytes

    let (numBlocks, check) = numBlockBytes `divMod` refSize
    dbugM $ "decodeExt: numBlocks = " ++ show numBlocks
    -- dbugM $ "decodeExt: numBlockBytes = " ++ show numBlockBytes
    -- dbugM $ "decodeExt: refSzie = " ++ show refSize
    -- dbugM $ "decodeExt: check = " ++ show check

    unless (check == 0) $ fail "Ext: Bad remaining byte count for block list."
    unless (not isEmbeddedExt && numBlocks >= minExtBlocks ||
            isEmbeddedExt     && numBlocks >= minInodeBlocks) $
      fail "Ext: Not enough space left for minimum number of blocks."

    blks <- filter (/= 0) `fmap` replicateM (safeToInt numBlocks) get

    checkMagic cmagic4
    padding <- replicateM cPadSize $ getWord8
    assert (all (== padSentinel) padding) $ return ()

    let na = error $ "numAddrs has not been populated via Data.Serialize.get "
                  ++ "for Ext; did you forget to use the "
                  ++ "Inode.decodeExt wrapper?"
    return Ext
           { address      = addr
           , nextExt      = ext
           , blockCount   = blkCnt
           , blockAddrs   = blks
           , numAddrs     = na
           }
   where
    checkMagic x = do
      magic <- getBytes 8
      unless (magic == x) $ fail "Invalid Ext: magic number mismatch"

--------------------------------------------------------------------------------
-- Debugging cruft

_dumpExts :: HalfsCapable b t r l m =>
              Ext -> HalfsM b r l m ()
_dumpExts stExt = do
  exts <- expandExts Nothing stExt
  if null exts
   then dbug ("=== No exts ===") $ return ()
   else do
     dbug ("=== Exts ===") $ return ()
     mapM_ (\c -> dbug ("  " ++ show c) $ return ()) exts

_allowNoUsesOf :: HalfsCapable b t r l m => HalfsM b r l m ()
_allowNoUsesOf = do
  extMapM undefined undefined >> return ()
