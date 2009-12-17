module Halfs.Directory
  (
   DirHandle(..)
  , FileStat(..)
  , FileMode(..)
  , AccessRight(..)
  , FileType(..)
  , findInDir
  , find
  , makeDirectory
  , openDirectory
  --
  , DirectoryEntry(..) -- FIXME! (don't export)
  , DirectoryState(..) -- FIXME! (don't export)
  )
 where

import Control.Monad
import Control.Exception(assert)
import qualified Data.ByteString as BS
import qualified Data.Map as M
import Data.Serialize
import Data.Word
import System.FilePath

import Halfs.Classes
import Halfs.Monad
import Halfs.Inode ( InodeRef(..)
                   , blockAddrToInodeRef
                   , buildEmptyInode
                   , drefInode
                   , readStream
                   )
import Halfs.Protection
import System.Device.BlockDevice

-- NB: These comments are not necc. up-to-date (FIXME)
--
-- Directories are stored like normal files, with a format of:
--   topLevelInode :: InodeRef
--   filename      :: String
--
-- In which file names are arbitrary-length, null-terminated strings. Valid file
-- names are guaranteed to not include null or the System.FilePath.pathSeparator
-- character. If a filename in the directory list contains
-- System.FilePath.pathSeparator as the first character, that means that the
-- file has been deleted but the directory itself hasn't been compressed.

data DirHandle r = DirHandle {
    dhInode       :: InodeRef
  , dhContents    :: r (M.Map FilePath DirectoryEntry)
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

data AccessRight    = Read | Write | Execute
data FileType       = RegularFile | Directory | Symlink deriving (Show, Eq)
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

-- | Given a block address to place the directory, its parent, its owner, and
-- its group, generate a new, empty directory with the given name (which is
-- Nothing in the case of the root directory).
makeDirectory :: HalfsCapable b t r l m =>
                 BlockDevice m
              -> Word64
              -> InodeRef
              -> Maybe String
              -> UserID
              -> GroupID
              -> m InodeRef
makeDirectory dev addr parentIR mdirname user group = do
  bstr <- buildEmptyInode dev thisIR parentIR user group
  -- sanity check
  assert (BS.length bstr == fromIntegral (bdBlockSize dev)) $ return ()
  -- end sanity check
  bdWriteBlock dev addr bstr
  case mdirname of
    Nothing    -> assert (thisIR == parentIR) $ return ()
    Just dname -> do
      -- TODO: Add dname to parent directory's contents
      -- i.e.: dh <- openDirectory dev parentIR
      --       (update contents and writeback and/or closeDir etc.)
      fail $ "TODO/NYI: makeDirectory add dirname="++dname++" to parent dir"
  return thisIR
  where
    thisIR = blockAddrToInodeRef addr

-- | Obtains an active directory handle for the directory at the given InodeRef
openDirectory :: HalfsCapable b t r l m =>
                 BlockDevice m -> InodeRef -> m (DirHandle r)
openDirectory dev inr = do
  inode <- drefInode dev inr

  -- Need a "data stream generator" that turns an inode into a lazy bytestring
  -- representing the underlying data that it covers, reading blocks and
  -- expanding inode continuations as needed...this will eventually end up being
  -- used by the file implementation as well

  -- TODO: DirHandle cache?  In HalfsState, perhaps?
  DirHandle inr `fmap` newRef contents `ap` newRef Clean
  where
    contents = undefined

{-
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

  data DirHandle r = DirHandle {
    dhInode       :: InodeRef
  , dhContents    :: r (M.Map FilePath DirectoryEntry)
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

-}

-- | Finds a directory, file, or symlink given a starting inode reference (i.e.,
-- the directory inode at which to begin the search) and a list of path
-- components.  Yields Nothing if any of the path components cannot be found in
-- the recursive descent of the directory hierarchy, otherwise yields the
-- InodeRef of the final path component.
find :: HalfsCapable b t r l m => 
        BlockDevice m -- ^ The block device to search
     -> InodeRef      -- ^ The starting inode reference
     -> FileType      -- ^ A match must be of this filetype
     -> [FilePath]    -- ^ Path components
     -> m (Maybe InodeRef)
--
find _ startINR _ [] = 
  return $ Just startINR
--
find dev startINR ftype (pathComp:rest) = do
  dh <- openDirectory dev startINR
  findInDir' dh pathComp ftype >>=
    maybe
      (return Nothing)
      (\de -> find dev (deInode de) ftype rest)

findInDir' :: HalfsCapable b t r l m =>
             DirHandle r
          -> String
          -> FileType
          -> m (Maybe DirectoryEntry)
findInDir' dh fname ftype =
  liftM (M.lookup fname) (readRef $ dhContents dh) >>= 
    maybe
      (return Nothing)
      (\de -> return $ if de `isFileType` ftype then Just de else Nothing)

-- Exportable version that doesn't expose DirectoryEntry
findInDir :: HalfsCapable b t r l m =>
               DirHandle r
            -> String
            -> FileType
            -> m (Maybe InodeRef)
findInDir dh fname ftype = 
  findInDir' dh fname ftype >>= return . maybe Nothing (Just . deInode)

--------------------------------------------------------------------------------
-- Utility functions

isFileType :: DirectoryEntry -> FileType -> Bool
isFileType (DirEnt { deType = t }) ft = t == ft
