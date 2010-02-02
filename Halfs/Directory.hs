module Halfs.Directory
  ( DirHandle(..)
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

import Control.Exception (assert)
import qualified Data.ByteString as BS
import qualified Data.Map as M
import Data.Serialize
import System.FilePath

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Errors
import Halfs.HalfsState
import Halfs.Monad
import Halfs.Inode ( InodeRef(..)
                   , blockAddrToInodeRef
                   , buildEmptyInodeEnc
                   , inodeRefToBlockAddr
                   , readStream
                   , writeStream
                   )
import Halfs.Protection
import Halfs.Types
import System.Device.BlockDevice

import Debug.Trace


--------------------------------------------------------------------------------
-- Directory manipulation and query functions

-- | Given a parent directory's inoderef, its owner, and its group,
-- generate a new, empty directory with the given name.
makeDirectory :: HalfsCapable b t r l m =>
                 HalfsState b r l m -- ^ the filesystem
              -> InodeRef           -- ^ inr to parent directory
              -> String             -- ^ directory name
              -> UserID             -- ^ user id for created directory
              -> GroupID            -- ^ group id for created directory
              -> FileMode           -- ^ initial filemode
              -> HalfsM m InodeRef  -- ^ on success, the inode ref to the
                                    --   created directory
makeDirectory fs parentIR dname user group perms = do 
  withDirectory fs parentIR $ \pdh -> do 
  contents <- readRef (dhContents pdh)
  trace ("makeDirectory: contents of parent directory are: " ++ show contents) $ do            
  if M.member dname contents
   then throwError HalfsDirectoryExists
   else do
     mir <- (fmap . fmap) blockAddrToInodeRef $ alloc1 (hsBlockMap fs)
     case mir of
       Nothing     -> throwError HalfsAllocFailed
       Just thisIR -> do
         trace ("makeDirectory: dname = " ++ show dname ++ " new inode @ " ++ show thisIR ) $ return ()
         
         -- Build the directory inode and persist it
         bstr <- lift $ buildEmptyInodeEnc dev thisIR parentIR user group
         assert (BS.length bstr == fromIntegral (bdBlockSize dev)) $ return ()
         lift $ bdWriteBlock dev (inodeRefToBlockAddr thisIR) bstr
       
         -- Add the new directory 'dname' to the parent dir's contents
         let newDE = DirEnt dname thisIR user group perms Directory
         writeRef (dhContents pdh) $ M.insert dname newDE contents
         modifyRef (dhState pdh) dirStTransAdd
    
         -- Persist the new directory structures
         syncDirectory fs pdh
         return thisIR
  where
    dev = hsBlockDev fs

-- | Syncs directory contents to disk

-- NB: This should be modified to use the DirHandle cache when it exists, in
-- which case this function may not take an explicit DirHandle to sync.
-- Expected behavior in that scenario would be to sync all non-Clean DirHandles.
-- Alternately, this might stay the same as a primitive op for CoreAPI.syncDir,
-- which might manage the set of DirHandles that need to be sync'd...
syncDirectory :: HalfsCapable b t r l m =>
                 HalfsState b r l m
              -> DirHandle r l
              -> HalfsM m ()
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
                 HalfsState b r l m
              -> InodeRef
              -> HalfsM m (DirHandle r l)
openDirectory fs inr = do
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

  rawDirBytes <- readStream (hsBlockDev fs) inr 0 Nothing
  dirEnts     <- if BS.null rawDirBytes
                 then do return []
                 else case decode rawDirBytes of 
                   Left msg -> throwError $ HalfsDecodeFail_Directory msg
                   Right x  -> return x
    
  contents <- return $ M.fromList $ map deName dirEnts `zip` dirEnts
--  trace ("openDirectory: DirHandle contents = " ++ show contents) $ do            
  DirHandle inr `fmap` newRef contents `ap` newRef Clean `ap` (newRef =<< newLock)

closeDirectory :: HalfsCapable b t r l m =>
                  HalfsState b r l m 
               -> DirHandle r l
               -> HalfsM m ()
closeDirectory fs dh = do
  -- TODO/design scratch re: locking and DirHandle cache
  syncDirectory fs dh
  return ()
  
-- | Add a directory entry for the given file to a directory; expects
-- that the file does not already exist in the directory
addFile :: HalfsCapable b t r l m =>
           DirHandle r l
        -> String
        -> InodeRef
        -> UserID
        -> GroupID
        -> FileMode
        -> HalfsM m ()
addFile dh fname fir u g mode = do
  -- begin sanity check
  mfound <- lkupDir fname dh
  maybe (return ()) (const $ throwError $ HalfsFileExists fname) mfound
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
        HalfsState b r l m -- ^ The filesystem to search
     -> InodeRef           -- ^ The starting inode reference
     -> FileType           -- ^ A match must be of this filetype
     -> [FilePath]         -- ^ Path components
     -> HalfsM m (Maybe InodeRef)
--
find _ startINR _ [] = 
  trace ("Directory.find base case, terminating") $ do
  return $ Just startINR
--
find fs startINR ftype (pathComp:rest) = do
  trace ("Directory.find recursive case, locating: " ++ show pathComp) $ do
  dh  <- openDirectory fs startINR
  mde <- findInDir' dh pathComp ftype
  case mde of
    Nothing -> return Nothing
    Just de -> find fs (deInode de) ftype rest

-- | Locate the given typed file by filename in the DirHandle's content map
findInDir' :: HalfsCapable b t r l m =>
              DirHandle r l
           -> String
           -> FileType
           -> HalfsM m (Maybe DirectoryEntry)
findInDir' dh fname ftype = do
  mde <- lkupDir fname dh
  case mde of
    Nothing -> return Nothing
    Just de -> return $ if de `isFileType` ftype then Just de else Nothing

-- Exportable version of findInDir; doesn't expose DirectoryEntry to caller
findInDir :: HalfsCapable b t r l m =>
             DirHandle r l
          -> String
          -> FileType
          -> HalfsM m (Maybe InodeRef)
findInDir dh fname ftype = 
  findInDir' dh fname ftype >>= return . maybe Nothing (Just . deInode)


--------------------------------------------------------------------------------
-- Utility functions

withDirectory :: HalfsCapable b t r l m =>
                 HalfsState b r l m
              -> InodeRef
              -> (DirHandle r l -> HalfsM m a)
              -> HalfsM m a
withDirectory fs ir = hbracket (openDirectory fs ir)
                               (closeDirectory fs)

lkupDir :: Reffable r m =>
           FilePath
        -> DirHandle r l
        -> m (Maybe DirectoryEntry)
lkupDir p dh = liftM (M.lookup p) (readRef $ dhContents dh)

isFileType :: DirectoryEntry -> FileType -> Bool
isFileType (DirEnt { deType = t }) ft = t == ft

_showDH :: Reffable r m => DirHandle r l -> m String
_showDH dh = do
  state    <- readRef $ dhState dh
  contents <- readRef $ dhContents dh
  return $ "DirHandle { dhInode    = " ++ show (dhInode dh)
                  ++ ", dhContents = " ++ show contents
                  ++ ", dhState    = " ++ show state

dirStTransAdd :: DirectoryState -> DirectoryState
dirStTransAdd Clean     = OnlyAdded
dirStTransAdd OnlyAdded = OnlyAdded
dirStTransAdd _         = VeryDirty

_dirStTransRm :: DirectoryState -> DirectoryState
_dirStTransRm Clean       = OnlyDeleted
_dirStTransRm OnlyDeleted = OnlyDeleted
_dirStTransRm _           = VeryDirty

dirStTransClean :: DirectoryState -> DirectoryState
dirStTransClean = const Clean

