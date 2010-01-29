module Halfs.Directory
  (
   DirHandle(..)
  , FileStat(..)
  , FileMode(..)
  , AccessRight(..)
  , FileType(..)
  , addFile
  , closeDirectory
  , find
  , findInDir
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

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Errors
import Halfs.Monad
import Halfs.Inode ( InodeRef(..)
                   , blockAddrToInodeRef
                   , buildEmptyInodeEnc
                   , inodeRefToBlockAddr
                   , readStream
                   , writeStream
                   )
import Halfs.Protection
import Halfs.SuperBlock (SuperBlock(rootDir))
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

-- | Given a parent directory's inoderef, its owner, and its group,
-- generate a new, empty directory with the given name.
makeDirectory :: HalfsCapable b t r l m =>
                 Halfs b r l m     -- ^ the filesystem
              -> InodeRef          -- ^ inr to parent directory
              -> String            -- ^ directory name
              -> UserID            -- ^ user id for created directory
              -> GroupID           -- ^ group id for created directory
              -> FileMode          -- ^ initial filemode
              -> HalfsM m InodeRef -- ^ on success, the inode ref to the
                                   --   created directory
makeDirectory fs parentIR dname user group perms = do
  -- TODO: Locking
  withDirectory fs parentIR $ \pdh -> do
  contents <- readRef (dhContents pdh)
  trace ("makeDirectory: contents of parent directory are: " ++ show contents) $ do            
  if M.member dname contents
   then do return $ Left HalfsDirectoryExists
   else do 
     mir <- (fmap . fmap) blockAddrToInodeRef $ alloc1 (hsBlockMap fs)
     case mir of
       Nothing     -> return $ Left HalfsAllocFailed
       Just thisIR -> do
         trace ("makeDirectory: creating dname = " ++ show dname ++ " new inode @ addr " ++ show thisIR ) $ do
         -- Build the directory inode and persist it
         bstr <- buildEmptyInodeEnc dev thisIR parentIR user group
         assert (BS.length bstr == fromIntegral (bdBlockSize dev)) $ return ()
         bdWriteBlock dev (inodeRefToBlockAddr thisIR) bstr
       
         -- Add the new directory 'dname' to the parent dir's contents
         let newDE = DirEnt dname thisIR user group perms Directory
         writeRef (dhContents pdh) $ M.insert dname newDE contents
         modifyRef (dhState pdh) dirStTransAdd
    
         -- NB/TODO: We write this back to disk immediately for now (via the
         -- explicit syncDir' invocation below), but presumably modifications to the
         -- directory state should be sufficient and syncing ought to happen
         -- elsewhere.
         syncDirectory fs pdh
         return $ Right thisIR
  where
    dev = hsBlockDev fs

-- | Syncs directory contents to disk

-- NB: This should be modified to use the DirHandle cache when it exists, in
-- which case this function may not take an explicit DirHandle to sync.
-- Expected behavior in that scenario would be to sync all non-Clean DirHandles.
-- Alternately, this might stay the same as a primitive op for CoreAPI.syncDir,
-- which might manage the set of DirHandles that need to be sync'd...
syncDirectory :: HalfsCapable b t r l m =>
                 Halfs b r l m -> DirHandle r -> m ()
syncDirectory fs dh = do 
  -- TODO: locking
  state <- readRef $ dhState dh
  case state of
    Clean       -> return ()
    OnlyAdded   -> do
      toWrite <- (encode . M.elems) `fmap` readRef (dhContents dh)

      tmpDbug <- M.elems `fmap` readRef (dhContents dh)                 
      trace ("syncDirectory: toWrite length = " ++ show (BS.length toWrite)
             ++ ", toWrite = " ++ show toWrite ++ "elems of dh contents = " ++ show tmpDbug) $ do

      -- TODO: Currently, we overwrite the entire DirectoryEntry list,
      -- truncating the directory's inode data stream -- this is braindead,
      -- though, as we should track the end of the stream and append
      writeStream (hsBlockDev fs) (hsBlockMap fs) (dhInode dh) 0 True toWrite
      modifyRef (dhState dh) dirStTransClean

    OnlyDeleted -> fail "syncDirectory for OnlyDeleted DirHandle state NYI"
    VeryDirty   -> fail "syncDirectory for VeryDirty DirHandle state NYI"

-- | Obtains an active directory handle for the directory at the given InodeRef
openDirectory :: HalfsCapable b t r l m =>
                 BlockDevice m -> InodeRef -> m (DirHandle r)
openDirectory dev inr = do
  -- TODO/design scratch re: locking and DirHandle cache.
 
  -- Should be: if we don't have a current DirHandle in the DH cache for this
  --   inr (and we may possibly need pathname here), read it in from disk and
  --   make a DirHandle (probably with its own acquired lock) to put into the
  --   cache and return.  Otherwise (found in cache), just hand it back after
  --   acquiring the lock?

  -- FIXME FIXME FIXME

  -- For now, read dir contents every time, and provide a new reference:
  -- NOT THREAD SAFE.  Furthermore, it's not even safe: multiple opens
  -- will result in new references, meaning the dirent map can get out
  -- of sync etc.  Gross.

  -- FIXME FIXME FIXME

  eRawDirBytes <- readStream dev inr 0 Nothing
  case eRawDirBytes of
    Left e            -> fail "readStream err"
    Right rawDirBytes -> do
      dirEnts <- if BS.null rawDirBytes
                 then do return []
                 else case decode rawDirBytes of 
                   Left msg -> fail $ "Failed to decode [DirectoryEntry]: "
                               ++ msg
                   Right x  -> return x
    
      contents <- return $ M.fromList $ map deName dirEnts `zip` dirEnts
      trace ("openDirectory: DirHandle contents = " ++ show contents) $ do            
      DirHandle inr `fmap` newRef contents `ap` newRef Clean

closeDirectory :: HalfsCapable b t r l m =>
                  Halfs b r l m 
               -> DirHandle r
               -> m ()
closeDirectory fs dh = do
  -- TODO/design scratch re: locking and DirHandle cache
  syncDirectory fs dh
  return ()
  
-- | Add a directory entry for the given file to a directory; expects
-- that the file does not already exist in the directory
addFile :: HalfsCapable b t r l m =>
           DirHandle r
        -> String
        -> InodeRef
        -> UserID
        -> GroupID
        -> FileMode
        -> m ()
addFile dh fname fir u g mode = do
  -- begin sanity check
  mfound <- liftM (M.lookup fname) (readRef $ dhContents dh)
  maybe (return ()) (fail "Directory.addFile: file already exists") mfound
  -- end sanity check

  -- TODO: DH locking: here?
  let de = DirEnt fname fir u g mode RegularFile
  modifyRef (dhContents dh) (M.insert fname de)
  modifyRef (dhState dh) dirStTransAdd

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
  dh  <- openDirectory dev startINR
  mde <- findInDir' dh pathComp ftype
  case mde of
    Nothing -> return Nothing
    Just de -> find dev (deInode de) ftype rest

-- | Locate the given typed file by filename in the DirHandle's content map
findInDir' :: HalfsCapable b t r l m =>
              DirHandle r
           -> String
           -> FileType
           -> m (Maybe DirectoryEntry)
findInDir' dh fname ftype = do
  mde <- liftM (M.lookup fname) (readRef $ dhContents dh)
  case mde of
    Nothing -> return Nothing
    Just de -> return $ if de `isFileType` ftype then Just de else Nothing

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

withDirectory :: HalfsCapable b t r l m =>
                 Halfs b r l m
              -> InodeRef
              -> (DirHandle r -> m a)
              -> m a
withDirectory fs ir act = do
  dh <- openDirectory (hsBlockDev fs) ir
  rslt <- act dh
  closeDirectory fs dh
  return rslt

isFileType :: DirectoryEntry -> FileType -> Bool
isFileType (DirEnt { deType = t }) ft = t == ft

showDH :: Reffable r m => DirHandle r -> m String
showDH dh = do
  state    <- readRef $ dhState dh
  contents <- readRef $ dhContents dh
  return $ "DirHandle { dhInode    = " ++ show (dhInode dh)
                  ++ ", dhContents = " ++ show contents
                  ++ ", dhState    = " ++ show state

dirStTransAdd :: DirectoryState -> DirectoryState
dirStTransAdd Clean     = OnlyAdded
dirStTransAdd OnlyAdded = OnlyAdded
dirStTransAdd _         = VeryDirty

dirStTransRm :: DirectoryState -> DirectoryState
dirStTransRm Clean       = OnlyDeleted
dirStTransRm OnlyDeleted = OnlyDeleted
dirStTransRm _           = VeryDirty

dirStTransClean :: DirectoryState -> DirectoryState
dirStTransClean = const Clean


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
