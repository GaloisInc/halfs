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
  -- * for testing
  , DirectoryEntry(..)
  , DirectoryState(..)
  )
 where

import Control.Applicative
import Control.Monad
import Control.Exception(assert)
import Data.Bits
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
                   , readStream
                   )
import Halfs.Protection
import System.Device.BlockDevice

import Debug.Trace

--------------------------------------------------------------------------------
-- Types & instances

-- File names are arbitrary-length, null-terminated strings.  Valid file names
-- are guaranteed to not include null or the System.FilePath.pathSeparator
-- character.

instance Serialize DirectoryEntry where
  put de = do
    put $ deName  de
    put $ deInode de
    put $ deUser  de
    put $ deGroup de
    put $ deMode  de
    put $ deType  de
  get = DirEnt <$> get <*> get <*> get <*> get <*> get <*> get

data DirectoryEntry = DirEnt {
    deName  :: String
  , deInode :: InodeRef
  , deUser  :: UserID
  , deGroup :: GroupID
  , deMode  :: FileMode
  , deType  :: FileType
  }

data DirHandle r = DirHandle {
    dhInode       :: InodeRef
  , dhContents    :: r (M.Map FilePath DirectoryEntry)
  , dhState       :: r DirectoryState
  }

data AccessRight    = Read | Write | Execute
  deriving (Show, Eq)

data DirectoryState = Clean | OnlyAdded | OnlyDeleted | VeryDirty
  deriving (Show, Eq)

instance Serialize FileMode where
  put FileMode{ fmOwnerPerms = op, fmGroupPerms = gp, fmUserPerms = up } = do
    when (any (>3) $ map length [op, gp, up]) $
      fail "Fixed-length check failed in FileMode serialization"
    putWord8 $ perms op
    putWord8 $ perms gp
    putWord8 $ perms up
    where
      perms ps  = foldr (.|.) 0x0 $ flip map ps $ \x -> -- toBit
                  case x of Read -> 0x4 ; Write -> 0x2; Execute -> 0x1
  --
  get = 
    FileMode <$> gp <*> gp <*> gp 
    where
      gp         = fromBits `fmap` getWord8
      fromBits x = let x0 = if testBit x 0 then [Execute] else []
                       x1 = if testBit x 1 then Write:x0  else x0
                       x2 = if testBit x 2 then Read:x1   else x1
                   in x2

data FileMode = FileMode {
    fmOwnerPerms :: [AccessRight]
  , fmGroupPerms :: [AccessRight]
  , fmUserPerms  :: [AccessRight]
  }


instance Serialize FileType where
  put RegularFile = putWord8 0x0
  put Directory   = putWord8 0x1
  put Symlink     = putWord8 0x2
  --
  get =
    getWord8 >>= \x -> case x of
      0x0 -> return RegularFile
      0x1 -> return Directory
      0x2 -> return Symlink
      _   -> fail "Invalid FileType during deserialize"

data FileType = RegularFile | Directory | Symlink
  deriving (Show, Eq)

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

--------------------------------------------------------------------------------
-- Directory manipulation and query functions

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
  -- Need a "data stream generator" that turns an inode into a lazy bytestring
  -- representing the underlying data that it covers, reading blocks and
  -- expanding inode continuations as needed...this will eventually end up being
  -- used by the file implementation as well

  rawDir <- readStream dev inr 0 Nothing
  -- HERE: decode rawDir after testing DirectoryEntry serialization instance

  -- TODO: DirHandle cache?  In HalfsState, perhaps?
  DirHandle inr `fmap` newRef contents `ap` newRef Clean
  where
    contents = M.empty

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

