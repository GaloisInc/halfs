module Halfs.Directory
  ( DirHandle(..)
  , FileStat(..)
  , FileMode(..)
  , AccessRight(..)
  , FileType(..)
  , addDirEnt
  , addDirEnt_lckd
  , addDirEnt_lckd'
  , closeDirectory
  , find
  , findInDir
  , getDHINR_lckd
  , makeDirectory
  , newDirHandle
  , openDirectory
  , removeDirectory
  , rmDirEnt
  , rmDirEnt_lckd
  , syncDirectory
  , syncDirectory_lckd
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
import Foreign.C.Error
import System.FilePath

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Errors
import Halfs.HalfsState
import Halfs.Monad
import Halfs.MonadUtils
import Halfs.Inode ( Inode(..)
                   , InodeRef(..)
                   , atomicReadInode
                   , blockAddrToInodeRef
                   , buildEmptyInodeEnc
                   , drefInode
                   , freeInode
                   , inodeRefToBlockAddr
                   , readStream
                   , withLockedInode
                   , writeStream
                   )
import Halfs.Protection
import Halfs.Types
import Halfs.Utils
import System.Device.BlockDevice

-- import Debug.Trace

type HalfsM b r l m a = HalfsT HalfsError (Maybe (HalfsState b r l m)) m a


--------------------------------------------------------------------------------
-- Directory manipulation and query functions

-- | Given a parent directory's inoderef, its owner, and its group,
-- generate a new, empty directory with the given name.
makeDirectory :: HalfsCapable b t r l m =>
                 InodeRef                 -- ^ inr to parent directory
              -> String                   -- ^ directory name
              -> UserID                   -- ^ user id for created directory
              -> GroupID                  -- ^ group id for created directory
              -> FileMode                 -- ^ initial perms for new directory
              -> HalfsM b r l m InodeRef  -- ^ on success, the inode ref to the
                                          --   created directory
makeDirectory parentIR dname user group perms =
  withDirectory parentIR $ \pdh -> do
    withDHLock pdh $ do 
      -- Begin critical section over parent's DirHandle 
      contents <- readRef (dhContents pdh)
      if M.member dname contents
       then throwError $ HE_ObjectExists dname 
       else do
         bm  <- hasks hsBlockMap 
         mir <- fmap blockAddrToInodeRef `fmap` alloc1 bm
         case mir of
           Nothing     -> throwError HE_AllocFailed
           Just thisIR -> do
             -- Build the directory inode and persist it
             dev  <- hasks hsBlockDev       
             bstr <- lift $ buildEmptyInodeEnc
                              dev
                              Directory
                              perms
                              thisIR
                              parentIR
                              user
                              group
             assert (BS.length bstr == fromIntegral (bdBlockSize dev)) $ do
             lift $ bdWriteBlock dev (inodeRefToBlockAddr thisIR) bstr
    
             -- Add 'dname' to parent directory's contents
             addDirEnt_lckd pdh dname thisIR user group perms Directory
             return thisIR
      -- End critical section over parent's DirHandle 

-- | Given a parent directory's inode ref, remove the directory with the given name.
removeDirectory :: HalfsCapable b t r l m =>
                   Maybe String -- ^ name to remove from parent
                                -- directory's content map (when Nothing,
                                -- leaves the the parent directory's
                                -- content map alone)
                -> InodeRef     -- ^ inr of directory to remove
                -> HalfsM b r l m ()
removeDirectory mdname inr = do
  -- TODO: Perms check (write perms on parent directory, etc.)
  dhMap <- hasks hsDHMap

  -- We lock the dirhandle map so (a) there's no contention for
  -- dirhandle lookup/creation for the directory we're removing and (b)
  -- so we can ensure that the directory is empty.

  withLockedRscRef dhMap $ \dhMapRef -> do
    dh <- lookupRM inr dhMapRef >>= maybe (newDirHandle inr) return
    withDHLock dh $ do
      -- begin dirhandle critical section   
      contents <- readRef (dhContents dh)
      unless (M.null contents) $ HE_DirectoryNotEmpty `annErrno` eNOTEMPTY
    
      -- When we've been given a directory name, purge this dir's dirent from
      -- the parent directory.
      case mdname of
        Nothing    -> return ()
        Just dname -> 
          withLockedInode inr $ do
            pinr <- inoParent `fmap` drefInode inr
            pdh  <- lookupRM pinr dhMapRef >>= maybe (newDirHandle pinr) return
            rmDirEnt pdh dname
    
      -- Invalidate dh so that all subsequent DH-mediated access fails
      writeRef (dhInode dh) Nothing
      deleteRM inr dhMapRef
      freeInode inr
      -- end dirhandle critical section   

-- | Syncs directory contents to disk
syncDirectory :: HalfsCapable b t r l m =>
                 DirHandle r l
              -> HalfsM b r l m ()
syncDirectory dh = withDHLock dh $ syncDirectory_lckd dh

syncDirectory_lckd :: HalfsCapable b t r l m =>
                      DirHandle r l
                   -> HalfsM b r l m ()
syncDirectory_lckd dh = do
  -- Precond: (dhLock dh) is currently held (can we assert this? TODO)
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
      writeStream inr 0 True
        =<< (encode . M.elems) `fmap` readRef (dhContents dh)
      lift . bdFlush =<< hasks hsBlockDev
      modifyRef (dhState dh) dirStTransClean

-- | Obtains an active directory handle for the directory at the given InodeRef
openDirectory :: HalfsCapable b t r l m =>
                 InodeRef
              -> HalfsM b r l m (DirHandle r l)
openDirectory inr = do
  -- TODO FIXME permissions checks!
  dhMap <- hasks hsDHMap
  mdh   <- withLockedRscRef dhMap (lookupRM inr)
  case mdh of
    Just dh -> return dh
    Nothing -> do
      dh <- newDirHandle inr
      withLockedRscRef dhMap $ \ref -> do
        -- If there's now a DirHandle in the map for our inode ref, prefer it to
        -- the one we just created; this is to safely avoid race conditions
        -- without extending the critical section over this entire function,
        -- which performs a potentially expensive BlockDevice read.
        mdh' <- lookupRM inr ref
        case mdh' of
          Just dh' -> return dh'
          Nothing  -> do
            insertRM inr dh ref
            return dh

closeDirectory :: HalfsCapable b t r l m =>
                  DirHandle r l
               -> HalfsM b r l m ()
closeDirectory dh = do
  syncDirectory dh
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
          -> HalfsM b r l m ()
addDirEnt dh name ir u g mode ftype =
  withDHLock dh $ addDirEnt_lckd dh name ir u g mode ftype

addDirEnt_lckd :: HalfsCapable b t r l m =>
                  DirHandle r l
               -> String
               -> InodeRef
               -> UserID
               -> GroupID
               -> FileMode
               -> FileType
               -> HalfsM b r l m ()
addDirEnt_lckd dh name inr u g mode ftype =
  addDirEnt_lckd' False dh $ DirEnt name inr u g mode ftype

addDirEnt_lckd' :: HalfsCapable b t r l m => 
                   Bool
                -> DirHandle r l
                -> DirectoryEntry
                -> HalfsM b r l m ()
addDirEnt_lckd' replaceOK dh de  = do
  -- Precond: (dhLock dh) is currently held (can we assert this? TODO)
  when (not replaceOK) $ do
    mfound <- lookupDE name dh
    maybe (return ()) (const $ throwError $ HE_ObjectExists name) mfound
  insertRM name de (dhContents dh)
  modifyRef (dhState dh) dirStTransAdd
  where
    name = deName de

-- | Remove a directory entry for a file, directory, or symlink; expects
-- that the item exists in the directory.  Thread-safe.
rmDirEnt :: HalfsCapable b t r l m =>
            DirHandle r l
         -> String
         -> HalfsM b r l m ()
rmDirEnt dh name =
  withDHLock dh $ rmDirEnt_lckd dh name

rmDirEnt_lckd :: HalfsCapable b t r l m =>
                 DirHandle r l
              -> String
              -> HalfsM b r l m ()
rmDirEnt_lckd dh name = do
  -- Precond: (dhLock dh) is currently held (can we assert this? TODO)
  -- begin sanity check
  mfound <- lookupDE name dh
  maybe (throwError $ HE_ObjectDNE name) (const $ return ()) mfound
  -- end sanity check
  deleteRM name (dhContents dh)
  modifyRef (dhState dh) dirStTransRm

-- | Finds a directory, file, or symlink given a starting inode
-- reference (i.e., the directory inode at which to begin the search)
-- and a list of path components.  Success is denoted using the DF_Found
-- constructor of the DirFindRslt type.
find :: HalfsCapable b t r l m => 
        InodeRef           -- ^ The starting inode reference
     -> FileType           -- ^ A match must be of this filetype
     -> [FilePath]         -- ^ Path components
     -> HalfsM b r l m (DirFindRslt InodeRef)
--
find startINR ftype [] = do
  ft <- atomicReadInode startINR inoFileType
  return $ foundRslt startINR ft ftype
--
find startINR ftype (pathComp:rest) = do
  dh <- openDirectory startINR
  sr <- findDE dh pathComp (if null rest then ftype else Directory)
  case sr of
    DF_NotFound         -> return $ DF_NotFound
    DF_WrongFileType ft -> return $ DF_WrongFileType ft
    DF_Found (de, _)    -> find (deInode de) ftype rest

-- | Locate the given directory entry typed file by filename in the
-- DirHandle's content map
findDE :: HalfsCapable b t r l m =>
          DirHandle r l
       -> String
       -> FileType
       -> HalfsM b r l m (DirFindRslt DirectoryEntry)
findDE dh fname ftype = do
  mde <- withDHLock dh $ lookupDE fname dh
  case mde of
    Nothing -> return DF_NotFound
    Just de -> return $ foundRslt de (deType de) ftype

-- Exportable version of findDE; doesn't expose DirectoryEntry to caller
findInDir :: HalfsCapable b t r l m =>
             DirHandle r l
          -> String
          -> FileType
          -> HalfsM b r l m (DirFindRslt InodeRef)
findInDir dh fname ftype = fmap deInode `fmap` findDE dh fname ftype

foundRslt :: a -> FileType -> FileType -> DirFindRslt a
foundRslt inr ft ftype =
  if ft `isFileType` ftype
   then DF_Found (inr, ft)
   else DF_WrongFileType ft


--------------------------------------------------------------------------------
-- Utility functions

newDirHandle :: HalfsCapable b t r l m =>
                InodeRef
             -> HalfsM b r l m (DirHandle r l)
newDirHandle inr = do
  rawDirBytes <- readStream inr 0 Nothing
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

-- Get directory handle's inode reference...
getDHINR_lckd :: HalfsCapable b t r l m =>
                 DirHandle r l
              -> HalfsM b r l m InodeRef
getDHINR_lckd dh = do
  -- Precond: (dhLock dh) has been acquired (TODO: can we assert this?)
  readRef (dhInode dh) >>= maybe (throwError HE_InvalidDirHandle) return

withDirectory :: HalfsCapable b t r l m =>
                 InodeRef
              -> (DirHandle r l -> HalfsM b r l m a)
              -> HalfsM b r l m a
withDirectory ir = hbracket (openDirectory ir) closeDirectory

isFileType :: FileType -> FileType -> Bool
isFileType _ AnyFileType = True
isFileType t1 t2         = t1 == t2

_showDH :: HalfsCapable b t r l m => DirHandle r l -> HalfsM b r l m String
_showDH dh = do
  withDHLock dh $ do 
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
