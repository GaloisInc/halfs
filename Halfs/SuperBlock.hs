module Halfs.SuperBlock(
         SuperBlock(..)
       , superBlockSize
       )
 where

import Control.Exception
import Halfs.Inode
import Data.ByteString(ByteString)
import qualified Data.ByteString as BS
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
superBlockSize = fromIntegral $ BS.length $ undefined {- encode blank -}
 where blank = SuperBlock 0 0 0 False 0 0 0 0 0

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

