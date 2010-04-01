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
  -- * for internal use only
  , addDirEnt_lckd
  , getDHINR_lckd
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
     mir <- fmap blockAddrToInodeRef `fmap` alloc1 (hsBlockMap fs)
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

-- | Given a parent directory's inode ref, remove the directory with the given name.
removeDirectory :: HalfsCapable b t r l m =>
                   HalfsState b r l m -- ^ the filesystem
                -> InodeRef           -- ^ inode of directory to remove
                -> HalfsM m ()
removeDirectory fs inr =
  -- We lock the dirhandle map so (a) there's no contention for
  -- dirhandle lookup/creation for the directory we're removing and (b)
  -- so we can ensure that the directory is empty.
  withLockedRscRef (hsDHMap fs) $ \dhMapRef -> do
  undefined -- HERE
    
--   withDirectory fs parentIR $ \pdh -> do
--   withLock (dhLock pdh) $ do
  -- Begin critical section over parent's DirHandle
  {-
    Sketch:

    - ensure that this dir's content map is empty, otherwise raise error

    - safely invalidate all directory handles by marking their InodeRefs
      invalid; this is so that any DH-mediated access that occurs in other
      threads will fail after this occurs.  openDirectory likely cannot do the
      1-2 punch of briefly locking the dhmap for lookup and then relocking later
      to see if one showed up...although maybe that's okay as long as the
      reference-based invalidation is working okay...

      key: lock dhmap (no new acquisitions of a DH for this inr), locked
      invalidation of the dirhandle associated with the dhmap, unlock dhmap.

      The idea is that, since the Maybe InodeRefs are locked resources, they
      can't be invalidated in the middle of someone else using them.
      Furthermore, if we acquire them in removeDirectory, we know that someone
      else can't, and since we'll be using the lockDH utility routine and errors
      will be raised elsewhere when they are acquired and the references
      indicate that they are bad...

      If I do decide to try to change opendirectory, though, see more notes
      below...

    - remove directory entry from parent content map

    - unallocate the inode entirely (new inode utility function to do this?)

    - invalidate any dirhandles that refer to this directory? we can probably
      lock the dhmap in order to do this.  we may need to do this first to avoid
      race conditions with openDirectory...?

      -----------------------------------------

      openDirectory needs to change :( right now, consider the following:

      dname is valid
      openDirectory from process A locks dhmap, does a lookup of an inr, releases (and returns dh to caller!)

      removeDirectory from process B locks dhmap, does all of the deletion,
      removes directory entirely including freeing its inode.

      ...

      process A openDirectory caller performs an operation on the dh: acquires
      lock and starts writing to the invalid inode (which could have already
      been reallocated etc.)

      The problem with this scenario is that even if the dhmap extends over the
      entire openDirectory operation, process A is still given a handle that
      holds only the inode reference, and this can be held indefinitely.  We
      need an independent way of determining when a directory handle has become
      invalid...it seems like what we need is for dirhandles to contain a
      reference to a Maybe InodeRef that can atomically become Nothing to
      indicate invalidation...?

      e.g., dhInode :: LockedRscRef l r (Maybe InodeRef)

      but this has more problems, because DirHandles are themselves by-value, so
      all we're going to do is end up creating new locks?

      a helper function like lockDH could be used to make the locking still very
      clean as with withLock.

  -}
  
  -- End critical section over parent's DirHandle  

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
  -- TODO: Currently, we overwrite the entire DirectoryEntry list, truncating
  -- the directory's inode data stream as needed.  This is _braindead_, however.
  -- For OnlyAdded, we can just append to the stream; for OnlyDeleted, we can
  -- write only invalidating entries and employ incremental coalescing, etc.
  -- overwriteAll should be reserved for the VeryDirty case only.
  case state of
    Clean       -> return ()
    OnlyAdded   -> overwriteAll
    OnlyDeleted -> overwriteAll
    VeryDirty   -> overwriteAll
  where
    overwriteAll = do
      inr <- getDHINR_lckd dh
      writeStream fs inr 0 True
        =<< (encode . M.elems) `fmap` readRef (dhContents dh)
      modifyRef (dhState dh) dirStTransClean

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
      dh <- newDirHandle fs inr
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


newDirHandle :: HalfsCapable b t r l m =>
                HalfsState b r l m
             -> InodeRef
             -> HalfsM m (DirHandle r l)
newDirHandle fs inr = do
  rawDirBytes <- readStream fs inr 0 Nothing
  dirEnts     <- if BS.null rawDirBytes
                 then do return []
                 else case decode rawDirBytes of 
                   Left msg -> throwError $ HE_DecodeFail_Directory msg
                   Right x  -> return x
  DirHandle
    `fmap` newRef (Just inr)
    `ap`   newRef (M.fromList $ map deName dirEnts `zip` dirEnts)
    `ap`   newRef Clean
    `ap`   newLock

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

addDirEnt_lckd :: HalfsCapable b t r l m =>
                  DirHandle r l
               -> String
               -> InodeRef
               -> UserID
               -> GroupID
               -> FileMode
               -> FileType
               -> HalfsM m ()
addDirEnt_lckd dh name inr u g mode ftype = do
  -- Precond: (dhLock dh) is currently held (can we assert this? TODO)
  -- begin sanity check
  mfound <- lookupRM name (dhContents dh)
  maybe (return ()) (const $ throwError $ HE_ObjectExists name) mfound
  -- end sanity check
  modifyRef (dhContents dh) (M.insert name $ DirEnt name inr u g mode ftype)
  modifyRef (dhState dh) dirStTransAdd

-- | Remove a directory entry for a file, directory, or symlink; expects
-- that the item exists in the directory.  Thread-safe.
rmDirEnt :: HalfsCapable b t r l m =>
            DirHandle r l
         -> String
         -> InodeRef
         -> HalfsM m ()
rmDirEnt dh name inr = 
  withLock (dhLock dh) $ rmDirEnt_lckd dh name inr

rmDirEnt_lckd :: HalfsCapable b t r l m =>
                 DirHandle r l
              -> String
              -> InodeRef
              -> HalfsM m ()
rmDirEnt_lckd dh name ir = do
  -- Precond: (dhLock dh) is currently held (can we assert this? TODO)
  -- begin sanity check
  mfound <- lookupRM name (dhContents dh)
  maybe (throwError $ HE_ObjectDNE name) (const $ return ()) mfound
  -- end sanity check
  modifyRef (dhContents dh) (M.delete name)
  modifyRef (dhState dh) dirStTransRm

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
findInDir dh fname ftype = fmap deInode `fmap` findDE dh fname ftype


--------------------------------------------------------------------------------
-- Utility functions

withDirectory :: HalfsCapable b t r l m =>
                 HalfsState b r l m
              -> InodeRef
              -> (DirHandle r l -> HalfsM m a)
              -> HalfsM m a
withDirectory fs ir = hbracket (openDirectory fs ir) (closeDirectory fs)

-- Get directory handle's inode reference...
getDHINR_lckd :: HalfsCapable b t r l m =>
                 DirHandle r l
              -> HalfsM m InodeRef
getDHINR_lckd dh = do
  -- Precond: (dhLock dh) has been acquired (TODO: can we assert this?)
  readRef (dhInode dh) >>= maybe (throwError HE_InvalidDirHandle) return
--  maybe (throwError HE_InvalidDirHandle) id `fmap` readRef (dhInode dh)

isFileType :: DirectoryEntry -> FileType -> Bool
isFileType _ AnyFileType              = True
isFileType (DirEnt { deType = t }) ft = t == ft

_showDH :: HalfsCapable b t r l m => DirHandle r l -> HalfsM m String
_showDH dh = do
  withLock (dhLock dh) $ do 
    state    <- readRef $ dhState dh
    contents <- readRef $ dhContents dh
    inr      <- getDHINR_lckd dh
    return $ "DirHandle { dhInode    = " ++ show inr
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
