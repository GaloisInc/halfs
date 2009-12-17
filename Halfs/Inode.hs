{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Halfs.Inode
  (
    Inode(..)
  , InodeRef(..)
  , blockAddrToInodeRef
  , buildEmptyInode
  , drefInode
  , inodeRefToBlockAddr
  , nilInodeRef
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
import Data.Serialize
import Data.Serialize.Get
import Data.Serialize.Put
import Data.Word

import Halfs.Classes
import Halfs.Protection
import System.Device.BlockDevice

-- ----------------------------------------------------------------------------

-- We store Inode reference as simple Word64, newtype'd in case we either
-- decide to do something more fancy or just to make the types a bit more
-- clear.
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

-- ----------------------------------------------------------------------------

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
  , parent        :: InodeRef        -- ^ block addr of parent directory inode
  , continuation  :: Maybe InodeRef
  , createTime    :: t
  , modifyTime    :: t
  , user          :: UserID
  , group         :: GroupID
  , numAddrs      :: Word64          -- ^ number of total block addresses
                                     -- that this inode covers
  , blockCnt      :: Word64          -- ^ current number of active blocks (equal
                                     -- to the number of inode references held
                                     -- in the blocks list)
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
             putWord64be $ blockCnt n
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

buildEmptyInode :: (Serialize t, Timed t m) =>
                   BlockDevice m ->
                   InodeRef -> InodeRef -> UserID -> GroupID ->
                   m ByteString
buildEmptyInode bd me mommy usr grp = do
  now     <- getTime
  minSize <- minimalInodeSize =<< return now
  nAddrs  <- computeNumAddrs (bdBlockSize bd) minSize
  return $ encode $ emptyInode nAddrs now now me mommy usr grp

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
    address       = me
  , parent        = mommy
  , continuation  = Nothing
  , createTime    = createTm
  , modifyTime    = modTm
  , user          = usr
  , group         = grp
  , numAddrs      = nAddrs
  , blockCnt      = 0
  , blocks        = []
  }

drefInode :: (Serialize t, Timed t m, Functor m) =>
             BlockDevice m -> InodeRef -> m (Inode t)
drefInode dev (IR addr) = 
  decode `fmap` bdReadBlock dev addr >>=
  either (fail . (++) "drefInode decode failure: ") return 

-- ----------------------------------------------------------------------------

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

