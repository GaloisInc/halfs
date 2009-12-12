module Halfs.SuperBlock(
         SuperBlock(..)
       , superBlockSize
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

import Halfs.Inode

data SuperBlock = SuperBlock {
       version       :: !Word64   -- ^ Version of this superblock
     , blockSize     :: !Word64   -- ^ Block size of underlying blkdev
     , blockCount    :: !Word64   -- ^ Number of blocks on underlying blkdev
     , unmountClean  :: !Bool     -- ^ Was this filesystem unmounted cleanly?
     , freeBlocks    :: !Word64   -- ^ Number of free blocks
     , usedBlocks    :: !Word64   -- ^ Number of blocks in use
     , fileCount     :: !Word64   -- ^ Number of files present in the FS; 
                                  --   does not count directories
     , rootDir       :: !InodeRef -- ^ IR to root directory
     , blockMapStart :: !InodeRef -- ^ IR to first block map addr
     }
  deriving (Show, Eq, Ord)

instance Serialize SuperBlock where
  put sb = do putByteString magic1
              putWord64be $ version sb
              putWord64be $ blockSize sb
              putWord64be $ blockCount sb
              putByteString magic2
              putWord64be $ if unmountClean sb then cleanMark else 0
              putWord64be $ freeBlocks sb
              putWord64be $ usedBlocks sb
              putByteString magic3
              putWord64be $ fileCount sb
              put         $ rootDir sb
              put         $ blockMapStart sb
              putByteString magic4
  get    = do checkMagic magic1
              v  <- getWord64be
              unless (v == 1) $ fail "Unsupported HALFS version found."
              bs <- getWord64be
              bc <- getWord64be
              checkMagic magic2
              uc <- wasUnmountedCleanly `fmap` getWord64be
              fb <- getWord64be
              ub <- getWord64be
              checkMagic magic3
              fc <- getWord64be
              rd <- get
              bm <- get
              checkMagic magic4
              return $ SuperBlock v bs bc uc fb ub fc rd bm
   where
    checkMagic x = do magic <- getBytes 8
                      unless (magic == x) $ fail "Invalid superblock."

superBlockSize :: Word64
superBlockSize = fromIntegral $ BS.length $ encode blank
 where blank = SuperBlock 0 0 0 False 0 0 0 0 0

-- ----------------------------------------------------------------------------

cleanMark :: Word64
cleanMark = 0x2357111317192329

wasUnmountedCleanly :: Word64 -> Bool
wasUnmountedCleanly x = x == cleanMark

magicStr :: String
magicStr = "The Haskell File System, Halfs!!"

magicBytes :: [Word8]
magicBytes = assert (length magicStr == 32) $
             map (fromIntegral . ord) magicStr

magic1, magic2, magic3, magic4 :: ByteString
magic1 = BS.pack $ take 8 $ drop  0 magicBytes
magic2 = BS.pack $ take 8 $ drop  8 magicBytes
magic3 = BS.pack $ take 8 $ drop 16 magicBytes
magic4 = BS.pack $ take 8 $ drop 24 magicBytes

