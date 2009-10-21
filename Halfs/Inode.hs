{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Halfs.Inode(
         Inode
       , InodeRef
       , blockAddrToInodeRef
       , inodeRefToBlockAddr
       , minimalInodeSize
       , buildEmptyInode
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

-- ----------------------------------------------------------------------------

-- The structure of an Inode. Pretty standard, except that we use the
-- continuation field to allow multiple runs of block addresses within
-- the file. We serialize Nothing as block 0, since this should be the
-- superblock and thus invalid as a continuation address.
--
-- We semi-arbitrarily state that an Inode must be capable of maintaining
-- a minimum of 47 block addresses, which gives us a minimum inode size of
-- 512 bytes.
--
minimumNumberOfBlocks :: Int
minimumNumberOfBlocks = 47

data (Eq t, Ord t, Serialize t) => Inode t = Inode {
    address       :: InodeRef
  , parent        :: InodeRef
  , continuation  :: Maybe InodeRef
  , createTime    :: t
  , modifyTime    :: t
  , user          :: UserID
  , group         :: GroupID
  , numAddrs      :: Word64
  , sizeBytes     :: Word64
  , sizeBlocks    :: Word64
  , liveSizeBytes :: Word64
  , blocks        :: [InodeRef]
  }

instance (Eq t, Ord t, Serialize t) => Serialize (Inode t) where
  put n = do putByteString magic1
             put $ address n
             put $ parent n
             let contVal = case continuation n of
                             Nothing -> blockAddrToInodeRef 0
                             Just x  -> x
             put contVal
             put $ createTime n
             put $ modifyTime n
             putByteString magic2
             put $ user n
             put $ group n
             putWord64be $ numAddrs n
             putWord64be $ sizeBytes n
             putWord64be $ sizeBlocks n
             putWord64be $ liveSizeBytes n
             putByteString magic3
             let blocks'    = blocks n
                 numAddrs'  = fromIntegral $ numAddrs n
                 numBlocks  = length blocks'
                 fillBlocks = numAddrs' - numBlocks
             unless (numBlocks <= numAddrs') $
               fail $ "Corrupted Inode structure: too many blocks"
             forM_ blocks' put
             replicateM_ fillBlocks $ put (IR 0)
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
             unless (mtm > ctm) $
               fail "Incoherent modified / creation times."
             checkMagic magic2
             u    <- get
             grp  <- get
             na   <- getWord64be
             sby  <- getWord64be
             sbl  <- getWord64be
             lsb  <- getWord64be
             checkMagic magic3
             remb <- remaining
             let numBlockBytes      = remb - 8
                 (numBlocks, check) = numBlockBytes `divMod` inodeRefSize
             unless (check == 0) $ 
               fail "Incorrect number of bytes left for block list."
             unless (numBlocks > minimumNumberOfBlocks) $
               fail "Not enough space left for minimum number of blocks."
             blks <- replicateM numBlocks get
             checkMagic magic4
             return $ Inode addr par cont ctm mtm u grp na sby sbl lsb blks
   where
    checkMagic x = do magic <- getBytes 8
                      unless (magic == x) $ fail "Invalid superblock."

minimalInodeSize :: (Serialize t, Timed t m) => m Word64
minimalInodeSize = do
  now <- getTime
  return $ fromIntegral $ BS.length $ encode $ emptyInode now
 where
  emptyInode now = Inode {
    address       = IR 0
  , parent        = IR 0
  , continuation  = Just (IR 0)
  , createTime    = now
  , modifyTime    = now
  , user          = rootUser
  , group         = rootGroup
  , numAddrs      = fromIntegral minimumNumberOfBlocks
  , sizeBytes     = 0
  , sizeBlocks    = 0
  , liveSizeBytes = 0
  , blocks        = replicate minimumNumberOfBlocks (IR 0)
  }

computeNumAddrs :: (Serialize t, Timed t m) =>
                   Word64 ->
                   m Word64
computeNumAddrs blockSize = do
  minSize <- minimalInodeSize
  let padding       = fromIntegral $ minimumNumberOfBlocks * inodeRefSize
      noBlockSize   = minSize - padding
      blockBytes    = blockSize - noBlockSize
      inodeRefSize' = fromIntegral inodeRefSize
  unless (blockBytes `mod` inodeRefSize' == 0) $
    fail "Inexplicably bad block size when computing number of blocks!"
  return $ blockBytes `div` inodeRefSize'

buildEmptyInode :: (Serialize t, Timed t m) =>
                   BlockDevice m ->
                   InodeRef -> InodeRef -> UserID -> GroupID ->
                   m ByteString
buildEmptyInode bd me mommy usr grp = do
  now <- getTime
  nAddrs <- computeNumAddrs (bdBlockSize bd)
  return $ encode $ emptyInode now nAddrs
 where
  emptyInode now nAddrs = Inode {
    address       = me
  , parent        = mommy
  , continuation  = Nothing
  , createTime    = now
  , modifyTime    = now
  , user          = usr
  , group         = grp
  , numAddrs      = nAddrs
  , sizeBytes     = 0
  , sizeBlocks    = 0
  , liveSizeBytes = 0
  , blocks        = []
  }

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

