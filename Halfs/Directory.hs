module Halfs.Directory
  ( DirHandle(..)
  , FileStat(..)
  , FileMode(..)
  , AccessRight(..)
  , FileType(..)
  , addDirEnt
  , closeDirectory
  , find
  , findInDir
  , makeDirectory
  , openDirectory
  , syncDirectory
  , withDirectory
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
import Halfs.Utils
import System.Device.BlockDevice


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
              -> FileMode           -- ^ initial perms for new directory
              -> HalfsM m InodeRef  -- ^ on success, the inode ref to the
                                    --   created directory
makeDirectory fs parentIR dname user group perms = 
  withDirectory fs parentIR $ \pdh -> do
  withLock (dhLock pdh) $ do 
  -- Begin critical section over parent's DirHandle 
  contents <- readRef (dhContents pdh)
  if M.member dname contents
   then throwError $ HE_ObjectExists dname 
   else do
     mir <- (fmap . fmap) blockAddrToInodeRef $ alloc1 (hsBlockMap fs)
     case mir of
       Nothing     -> throwError HE_AllocFailed
       Just thisIR -> do
         -- Build the directory inode and persist it
         bstr <- lift $ buildEmptyInodeEnc
                          dev
                          Directory
                          perms
                          thisIR
                          parentIR
                          user
                          group
         assert (BS.length bstr == fromIntegral (bdBlockSize dev)) $ return ()
         lift $ bdWriteBlock dev (inodeRefToBlockAddr thisIR) bstr
       
         -- Add 'dname' to parent directory's contents
         addDirEnt_lckd pdh dname thisIR user group perms Directory
         return thisIR
  -- End critical section over parent's DirHandle 
  where
    dev = hsBlockDev fs

-- | Syncs directory contents to disk

-- NB: We need to decide where all open & dirty DirHandles are sync'd.  Probably
-- in fs unmount/teardown via CoreAPI.
syncDirectory :: HalfsCapable b t r l m =>
                 HalfsState b r l m
              -> DirHandle r l
              -> HalfsM m ()
syncDirectory fs dh = do 
  withLock (dhLock dh) $ do 
  state <- readRef $ dhState dh
  case state of
    Clean       -> return ()
    OnlyAdded   -> do
      toWrite <- (encode . M.elems) `fmap` readRef (dhContents dh)
  
      -- TODO: Currently, we overwrite the entire DirectoryEntry list,
      -- truncating the directory's inode data stream as needed -- this is
      -- braindead, though.  Instead, we should do something like tracking the
      -- end of the stream and appending new contents there.

      writeStream fs (dhInode dh) 0 True toWrite
      modifyRef (dhState dh) dirStTransClean
  
    OnlyDeleted -> fail "syncDirectory for OnlyDeleted DirHandle state NYI"
    VeryDirty   -> fail "syncDirectory for VeryDirty DirHandle state NYI"

-- | Obtains an active directory handle for the directory at the given InodeRef
openDirectory :: HalfsCapable b t r l m =>
                 HalfsState b r l m
              -> InodeRef
              -> HalfsM m (DirHandle r l)
openDirectory fs inr = do
  -- TODO: Consider potential race conditions / danging dirhandle ref
  -- problems here, e.g. w.r.t. directory removal being interleaved
  -- immediately after the dhmap lock acquisition/release window via
  -- withLockedRscRef.  What can go wrong? Is the subsequent lookup on
  -- reacq sufficient to mitigate the problem?
  --
  -- NB: Even when there are no race conditions here, We'll need a clean way to
  -- invalidate DirHandles that escape from here, so that users of the handle
  -- are informed when their handle is dangling (which can occur, e.g., if
  -- another process removes a directory).

  -- TODO FIXME permissions checks!

  mdh <- withLockedRscRef (hsDHMap fs) (lookupRM inr)
  case mdh of
    Just dh -> return dh
    Nothing -> do
      -- No DirHandle for this InodeRef, so pull in the directory info
      -- from the dev and make one.
      rawDirBytes <- readStream fs inr 0 Nothing
      dirEnts     <- if BS.null rawDirBytes
                     then do return []
                     else case decode rawDirBytes of 
                       Left msg -> throwError $ HE_DecodeFail_Directory msg
                       Right x  -> return x
      dh <- DirHandle inr
              `fmap` newRef (M.fromList $ map deName dirEnts `zip` dirEnts)
              `ap`   newRef Clean
              `ap`   newLock
      withLockedRscRef (hsDHMap fs) $ \ref -> do
        -- If there's now a DirHandle in the map for our inode ref, prefer it to
        -- the one we just created; this is to safely avoid race conditions
        -- without extending the critical section over this entire function,
        -- which performs a potentially expensive BlockDevice read.
        mdh' <- lookupRM inr ref
        case mdh' of
          Just dh' -> return dh'
          Nothing  -> do
            modifyRef ref (M.insert inr dh)
            return dh

closeDirectory :: HalfsCapable b t r l m =>
                  HalfsState b r l m 
               -> DirHandle r l
               -> HalfsM m ()
closeDirectory fs dh = do
  syncDirectory fs dh
  return ()
  
-- | Add a directory entry for a file, directory, or symlink; expects
-- that the item does not already exist in the directory.  Thread-safe.
addDirEnt :: HalfsCapable b t r l m =>
             DirHandle r l
          -> String
          -> InodeRef
          -> UserID
          -> GroupID
          -> FileMode
          -> FileType
          -> HalfsM m ()
addDirEnt dh name ir u g mode ftype =
  withLock (dhLock dh) $ addDirEnt_lckd dh name ir u g mode ftype
  -- By default, always acquire the DirHandle lock!

addDirEnt_lckd :: HalfsCapable b t r l m =>
                  DirHandle r l
               -> String
               -> InodeRef
               -> UserID
               -> GroupID
               -> FileMode
               -> FileType
               -> HalfsM m ()
addDirEnt_lckd dh name ir u g mode ftype = do
  -- Precond: (dhLock dh) is currently held (can we assert this? TODO)
  -- begin sanity check
  mfound <- lookupRM name (dhContents dh)
  maybe (return ()) (const $ throwError $ HE_ObjectExists name) mfound
  -- end sanity check
  modifyRef (dhContents dh) (M.insert name $ DirEnt name ir u g mode ftype)
  modifyRef (dhState dh) dirStTransAdd

addDot :: HalfsCapable b t r l m =>
          DirHandle r l
       -> UserID
       -> GroupID
       -> FileMode
       -> HalfsM m ()
addDot dh u g mode = addDirEnt dh "." (dhInode dh) u g mode Directory

-- | Finds a directory, file, or symlink given a starting inode
-- reference (i.e., the directory inode at which to begin the search)
-- and a list of path components.  Success is denoted using the DF_Found
-- constructor of the DirFindRslt type.
find :: HalfsCapable b t r l m => 
        HalfsState b r l m -- ^ The filesystem to search
     -> InodeRef           -- ^ The starting inode reference
     -> FileType           -- ^ A match must be of this filetype
     -> [FilePath]         -- ^ Path components
     -> HalfsM m (DirFindRslt InodeRef)
--
find _ startINR _ [] = 
  return $ DF_Found startINR
--
find fs startINR ftype (pathComp:rest) = do
  dh <- openDirectory fs startINR
  sr <- findDE dh pathComp (if null rest then ftype else Directory)
  case sr of
    DF_NotFound         -> return $ DF_NotFound
    DF_WrongFileType ft -> return $ DF_WrongFileType ft
    DF_Found de         -> find fs (deInode de) ftype rest

-- | Locate the given directory entry typed file by filename in the
-- DirHandle's content map
findDE :: HalfsCapable b t r l m =>
          DirHandle r l
       -> String
       -> FileType
       -> HalfsM m (DirFindRslt DirectoryEntry)
findDE dh fname ftype = do
  mde <- withLock (dhLock dh) $ lookupRM fname (dhContents dh)
  case mde of
    Nothing -> return DF_NotFound
    Just de -> return $ if de `isFileType` ftype
                         then DF_Found de
                         else DF_WrongFileType (deType de)

-- Exportable version of findDE; doesn't expose DirectoryEntry to caller
findInDir :: HalfsCapable b t r l m =>
             DirHandle r l
          -> String
          -> FileType
          -> HalfsM m (DirFindRslt InodeRef)
findInDir dh fname ftype = (fmap . fmap) deInode (findDE dh fname ftype)


--------------------------------------------------------------------------------
-- Utility functions

withDirectory :: HalfsCapable b t r l m =>
                 HalfsState b r l m
              -> InodeRef
              -> (DirHandle r l -> HalfsM m a)
              -> HalfsM m a
withDirectory fs ir = hbracket (openDirectory fs ir)
                               (closeDirectory fs)

isFileType :: DirectoryEntry -> FileType -> Bool
isFileType _ AnyFileType              = True
isFileType (DirEnt { deType = t }) ft = t == ft

_showDH :: HalfsCapable b t r l m => DirHandle r l -> HalfsM m String
_showDH dh = do
  withLock (dhLock dh) $ do 
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
