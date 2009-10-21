module Halfs.Directory (
         DirHandle(..)
       , FileStat(..)
       , FileMode(..)
       , AccessRight(..)
       , FileType(..)
       , makeDirectory
       --
       , DirectoryEntry(..) -- FIXME! (don't export)
       , DirectoryState(..) -- FIXME! (don't export)
       )
 where

import Control.Exception(assert)
import qualified Data.ByteString as BS
import Data.Map(Map)
import Data.Serialize
import Data.Word
import System.FilePath

import Halfs.Classes
import Halfs.Inode
import Halfs.Protection
import System.Device.BlockDevice

-- Directories are stored like normal files, with a format of:
--   topLevelInode :: InodeRef
--   filename      :: String
--
-- In which file names are arbitrary-length, null-terminated strings. Valid
-- file names are guaranteed to not include null or the '/' character. If
-- a filename in the directory list contains '/' as the first character, that
-- means that the file has been deleted but the directory itself hasn't been
-- compressed.

data DirHandle r = DirHandle {
    dhInode       :: InodeRef
  , dhContents    :: r (Map FilePath DirectoryEntry)
  , dhState       :: r DirectoryState
  }

data DirectoryEntry = DirEnt {
    deName  :: String
  , deInode :: InodeRef
  , deUser  :: UserID
  , deGroup :: GroupID
  , deMode  :: Int
  , deType  :: FileType
  }

data AccessRight = Read | Write | Execute
data FileType    = RegularFile | Directory | Symlink
data DirectoryState = Clean | OnlyAdded | OnlyDeleted | VeryDirty

data FileMode = FileMode {
    fmOwnerPerms :: [AccessRight]
  , fmGroupPerms :: [AccessRight]
  , fmUserPerms  :: [AccessRight]
  }

data FileStat t = FileStat {
    fsInode      :: InodeRef
  , fsType       :: FileType
  , fsMode       :: FileMode
  , fsLinks      :: Word64
  , fsUID        :: UserID
  , fsGID        :: GroupID
  , fsSize       :: Word64
  , fsNumBlocks  :: Word64
  , fsAccessTime :: t
  , fsModTime    :: t
  , fsChangeTime :: t
  }

-- |Given a block address to place the directory, its parent, its owner, and
-- its group, generate a new, empty directory.
makeDirectory :: (Serialize t, Timed t m) =>
                 BlockDevice m -> Word64 -> InodeRef -> UserID -> GroupID ->
                 m InodeRef
makeDirectory bd addr mommy user group = do
  bstr <- buildEmptyInode bd (blockAddrToInodeRef addr) mommy user group
  -- sanity check
  let bsize = fromIntegral $ bdBlockSize bd
  assert (BS.length bstr == bsize) $ return ()
  -- end sanity check
  bdWriteBlock bd addr bstr
  return (blockAddrToInodeRef addr)
