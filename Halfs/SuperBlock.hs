module Halfs.SuperBlock(
         SuperBlock(..)
       , superBlockSize
       )
 where

import Control.Exception
import Halfs.Inode
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Lazy(ByteString)
import qualified Data.ByteString.Lazy as BS
import Data.Char
import Data.Word

data SuperBlock = SuperBlock {
       version      :: !Word64
     , blockSize    :: !Word64
     , blockCount   :: !Word64
     , unmountClean :: !Bool
     , freeBlocks   :: !Word64
     , usedBlocks   :: !Word64
     , fileCount    :: !Word64
     , rootDir      :: !InodeRef
     , blockList    :: !InodeRef
     }

superBlockSize :: Word64
superBlockSize = fromIntegral $ BS.length $ encode blank
 where blank = SuperBlock 0 0 0 False 0 0 0 0 0

instance Binary SuperBlock where
  get    = do m1 <- getLazyByteString 8
              v  <- get
              bs <- get
              bc <- get
              m2 <- getLazyByteString 8
              uc <- get
              fb <- get
              ub <- get
              m3 <- getLazyByteString 8
              fc <- get
              rd <- get
              bl <- get
              m4 <- getLazyByteString 8
              return $! SuperBlock v bs bc (uc == cleanMark) fb ub fc rd bl
  put sb = do putLazyByteString magic1
              put $ version sb
              put $ blockSize sb
              put $ blockCount sb
              putLazyByteString magic2
              put $ if unmountClean sb then cleanMark else 0
              put $ freeBlocks sb
              put $ usedBlocks sb
              putLazyByteString magic3
              put $ fileCount sb
              put $ rootDir sb
              put $ blockList sb
              putLazyByteString magic4

-- ----------------------------------------------------------------------------

cleanMark :: Word64
cleanMark = 0x2357111317192329

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

