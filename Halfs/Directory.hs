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
import Control.Exception (assert)
import Data.Bits
import qualified Data.ByteString as BS
import Data.List (sort)
import qualified Data.Map as M
import Data.Serialize
import Data.Word
import System.FilePath

import Halfs.Classes
import Halfs.Errors
import Halfs.Monad
import Halfs.Inode ( InodeRef(..)
                   , blockAddrToInodeRef
                   , buildEmptyInode
                   , readStream
                   , writeStream
                   )
import Halfs.Protection
import Halfs.Utils
import System.Device.BlockDevice

import Debug.Trace


--------------------------------------------------------------------------------
-- Types

-- File names are arbitrary-length, null-terminated strings.  Valid file names
-- are guaranteed to not include null or the System.FilePath.pathSeparator
-- character.

data DirectoryEntry = DirEnt {
    deName  :: String
  , deInode :: InodeRef
  , deUser  :: UserID
  , deGroup :: GroupID
  , deMode  :: FileMode
  , deType  :: FileType
  }
  deriving (Show, Eq)

data DirHandle r = DirHandle {
    dhInode       :: InodeRef
  , dhContents    :: r (M.Map FilePath DirectoryEntry)
  , dhState       :: r DirectoryState
  }

data AccessRight = Read | Write | Execute
  deriving (Show, Eq, Ord)

data DirectoryState = Clean | OnlyAdded | OnlyDeleted | VeryDirty
  deriving (Show, Eq)

data FileMode = FileMode {
    fmOwnerPerms :: [AccessRight]
  , fmGroupPerms :: [AccessRight]
  , fmUserPerms  :: [AccessRight]
  }
  deriving (Show)

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
                 Halfs b r m l
              -> Word64
              -> InodeRef
              -> String
              -> UserID
              -> GroupID
              -> FileMode 
              -> HalfsM m InodeRef
makeDirectory fs addr parentIR dname user group perms = do
  -- Build the directory inode and persist it
  bstr <- buildEmptyInode dev thisIR parentIR user group
  assert (BS.length bstr == fromIntegral (bdBlockSize dev)) $ do 
  bdWriteBlock dev addr bstr

  -- Add the new directory 'dname' to the parent diretory's contents
  -- TODO: Locking

  dh       <- openDirectory dev parentIR
  contents <- readRef (dhContents dh)
  if M.member dname contents
   then return $ Left HalfsDirectoryExists
   else do 
  let newDE = DirEnt dname thisIR user group perms Directory
  writeRef (dhContents dh) $ M.insert dname newDE contents
  modifyRef (dhState dh) dirStTransAdd

  -- NB/TODO: We write this back to disk immediately for now (via the explicit
  -- syncDir' invocation below), but presumably modifications to the directory
  -- state should be sufficient and syncing ought to happen elsewhere.

  syncDirectory fs dh
  return $ Right thisIR
  where
    thisIR = blockAddrToInodeRef addr
    dev    = hsBlockDev fs

-- | Syncs contents referred to by a directory handle to the disk

-- NB: This should probably be modified to use the DirHandle cache when it
-- exists, in which case this function may not take an explicit DirHandle to
-- sync.  Expected behavior in that scenario would be to sync all non-Clean
-- DirHandles.  Alternately, this might stay the same as a primitive op for
-- CoreAPI.syncDir, which might manage the set of DirHandles that need to be
-- sync'd...
syncDirectory :: HalfsCapable b t r l m =>
                 Halfs b r m l -> DirHandle r -> HalfsM m ()
syncDirectory fs dh = do 
  -- TODO: locking
  state <- readRef $ dhState dh
  case state of
    Clean       -> return $ Right ()
    OnlyAdded   -> do
      toWrite <- (encode . M.elems) `fmap` readRef (dhContents dh)
      trace ("syncDirectory: toWrite length = " ++ show (BS.length toWrite)
             ++ ", toWrite = " ++ show toWrite) $ do
      -- Overwrite the entire DirectoryEntry list, truncating the directory's
      -- inode data stream
      writeStream (hsBlockDev fs) (hsBlockMap fs) (dhInode dh) 0 True toWrite
      return $ Right ()

    OnlyDeleted -> fail "syncDirectory for OnlyDeleted DirHandle states NYI"
    VeryDirty   -> fail "syncDirectory for VeryDirty DirHandle states NYI"

  return $ Right () 

-- | Obtains an active directory handle for the directory at the given InodeRef
openDirectory :: HalfsCapable b t r l m =>
                 BlockDevice m -> InodeRef -> m (DirHandle r)
openDirectory dev inr = do
  -- Need a "data stream generator" that turns an inode into a lazy bytestring
  -- representing the underlying data that it covers, reading blocks and
  -- expanding inode continuations as needed...this will eventually end up being
  -- used by the file implementation as well: this is 'Inode.readStream' below

  -- TODO: May want to allocate a block for empty directories and serialize an
  -- empty list so that this null size checking does not need to occur, nor the
  -- special case in readStream.
  rawDirBytes <- readStream dev inr 0 Nothing
  dirEnts     <- if BS.null rawDirBytes
                 then return []
                 else decode `fmap` readStream dev inr 0 Nothing >>=
                      either (fail . (++) "Failed to decode [DirectoryEntry]: ")
                             (return)
  trace ("dirEnts = " ++ show (dirEnts :: [DirectoryEntry])) $ do

  -- HERE: Next: Add a single directory to the root level

  -- TODO: DirHandle cache?  In HalfsState, perhaps?
  DirHandle inr `fmap` newRef contents `ap` newRef Clean
  where
    contents = M.empty -- FIXME: 

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
  trace ("Directory.find base case, terminating") $ do
  return $ Just startINR
--
find dev startINR ftype (pathComp:rest) = do
  trace ("Directory.find recursive case, locating: " ++ show pathComp) $ do
  dh <- openDirectory dev startINR
  findInDir' dh pathComp ftype >>=
    maybe
      (return Nothing)
      (\de -> find dev (deInode de) ftype rest)

-- | Locate the given typed file by filename in the dirhandle's content map
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

-- Exportable version of findInDir; doesn't expose DirectoryEntry to caller
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

showDH :: Reffable r m => DirHandle r -> m String
showDH dh = do
  state    <- readRef $ dhState dh
  contents <- readRef $ dhContents dh
  return $ "DirHandle { dhInode = " ++ show (dhInode dh)
                  ++ ", dhContents = " ++ show contents
                  ++ ", dhState = " ++ show state

dirStTransAdd :: DirectoryState -> DirectoryState
dirStTransAdd Clean     = OnlyAdded
dirStTransAdd OnlyAdded = OnlyAdded
dirStTransAdd _         = VeryDirty

dirStTransRm :: DirectoryState -> DirectoryState
dirStTransRm Clean       = OnlyDeleted
dirStTransRm OnlyDeleted = OnlyDeleted
dirStTransRm _           = VeryDirty


--------------------------------------------------------------------------------
-- Instances

instance Serialize DirectoryEntry where
  put de = do
    put $ deName  de
    put $ deInode de
    put $ deUser  de
    put $ deGroup de
    put $ deMode  de
    put $ deType  de
  get = DirEnt <$> get <*> get <*> get <*> get <*> get <*> get

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

instance Serialize FileMode where
  put FileMode{ fmOwnerPerms = op, fmGroupPerms = gp, fmUserPerms = up } = do
    when (any (>3) $ map length [op, gp, up]) $
      fail "Fixed-length check failed in FileMode serialization"
    putWord8 $ perms op
    putWord8 $ perms gp
    putWord8 $ perms up
    where
      perms ps  = foldr (.|.) 0x0 $ flip map ps $ \x -> -- toBit
                  case x of Read -> 4 ; Write -> 2; Execute -> 1
  --
  get = 
    FileMode <$> gp <*> gp <*> gp 
    where
      gp         = fromBits `fmap` getWord8
      fromBits x = let x0 = if testBit x 0 then [Execute] else []
                       x1 = if testBit x 1 then Write:x0  else x0
                       x2 = if testBit x 2 then Read:x1   else x1
                   in x2

instance Eq FileMode where
  fm1 == fm2 =
    sort (fmOwnerPerms fm1) == sort (fmOwnerPerms fm2) &&
    sort (fmGroupPerms fm1) == sort (fmGroupPerms fm2) &&
    sort (fmUserPerms  fm1) == sort (fmUserPerms  fm2)
