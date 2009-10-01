{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Halfs.Inode(
         Inode(..)
       , InodeRef
       , blockAddrToInodeRef
       , inodeRefToBlockAddr
       )
 where

import Control.Exception
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
import Data.Char
import Data.Time.Clock
import Data.Word

import Halfs.Classes
import Halfs.Protection

newtype InodeRef = IR Word64
  deriving (Eq, Ord, Num, Show, Integral, Enum, Real)

blockAddrToInodeRef :: Word64 -> InodeRef
blockAddrToInodeRef = IR

inodeRefToBlockAddr :: InodeRef -> Word64
inodeRefToBlockAddr (IR x) = x

data (Eq t, Ord t) => Inode t = Inode {
    address       :: InodeRef
  , parent        :: InodeRef
  , continuation  :: Maybe InodeRef
  , createTime    :: t
  , modifyTime    :: t
  , user          :: UserID
  , group         :: GroupID
  , sizeBytes     :: Word64
  , sizeBlocks    :: Word64
  , liveSizeBytes :: Word64
  , blocks        :: [InodeRef]
  }

minimalInodeSize :: Timed t m => m Word64
minimalInodeSize = undefined
{-  now <- getTime
  return $ fromIntegral $ BS.length $ encode $ emptyInode now -}
 where
  emptyInode now = Inode {
    address       = IR 0
  , parent        = IR 0
  , continuation  = Just (IR 0)
  , createTime    = now
  , modifyTime    = now
  , user          = rootUser
  , group         = rootGroup
  , sizeBytes     = 0
  , sizeBlocks    = 0
  , liveSizeBytes = 0
  , blocks        = replicate 16 (IR 0)
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

