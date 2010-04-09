{-# LANGUAGE FlexibleContexts, ScopedTypeVariables, RankNTypes #-}
module Halfs.CoreAPI where

import Control.Exception (assert)
import Data.ByteString   (ByteString)
import qualified Data.ByteString  as BS
import qualified Data.Map         as M
import Data.Serialize
import qualified Data.Traversable as T
import Data.Word
import Foreign.C.Error hiding (throwErrno)
import System.FilePath

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Directory
import Halfs.Errors
import Halfs.File
import qualified Halfs.File as F
import Halfs.HalfsState
import Halfs.Inode
import Halfs.Monad
import Halfs.MonadUtils
import Halfs.Protection
import Halfs.SuperBlock
import Halfs.Types
import Halfs.Utils

import System.Device.BlockDevice

import Debug.Trace

type HalfsM b r l m a = HalfsT HalfsError (Maybe (HalfsState b r l m)) m a

data SyncType = Data | Everything

data FileSystemStats = FSS
  { fssBlockSize   :: Integer -- ^ fundamental file system block size
  , fssBlockCount  :: Integer -- ^ #data blocks in filesystem
  , fssBlocksFree  :: Integer -- ^ #free blocks in filesystem
  , fssBlocksAvail :: Integer -- ^ #free blocks avail to non-root
  , fssFileCount   :: Integer -- ^ #num inodes in filesystem
  , fssFilesFree   :: Integer -- ^ #free inodes in filesystem
  , fssFilesAvail  :: Integer -- ^ #free inodes avail to non-root
  }
  deriving (Show)


--------------------------------------------------------------------------------
-- Filesystem init, teardown, and check functions

-- | Create a new file system on the given device. The new file system
-- will use the block size reported by bdBlockSize on the device; if you
-- want to increase the file system's block size, you should use
-- 'newRescaledBlockDevice' from System.Device.BlockDevice.
--
-- Block size has no bearing on the maximum size of a file or directory, but
-- will impact the size of certain internal data structures that keep track of
-- free blocks. Larger block sizes will decrease the cost of these structures
-- per underlying disk megabyte, but may also lead to more wasted space for
-- small files and directories.

newfs :: (HalfsCapable b t r l m) =>
         BlockDevice m
      -> UserID
      -> GroupID
      -> FileMode
      -> HalfsM b r l m SuperBlock
newfs dev uid gid rdirPerms = do
  when (superBlockSize > bdBlockSize dev) $
    fail "The device's block size is insufficiently large!"

  blockMap <- lift $ newBlockMap dev
  initFree <- readRef (bmNumFree blockMap)
  rdirAddr <- lift (alloc1 blockMap) >>=
                maybe (fail "Unable to allocate block for rdir inode") return

  -- Build the root directory inode and persist it; note that we can't use
  -- Directory.makeDirectory here because this is a special case where we have
  -- no parent directory (and, furthermore, no filesystem state).
  let rdirIR = blockAddrToInodeRef rdirAddr
  dirInode <- lift $
    buildEmptyInodeEnc 
      dev
      Directory
      rdirPerms
      rdirIR
      nilInodeRef
      uid
      gid
  assert (BS.length dirInode == fromIntegral (bdBlockSize dev)) $ do
  lift $ bdWriteBlock dev rdirAddr dirInode

  finalFree <- readRef (bmNumFree blockMap)
  -- Persist the remaining data structures
  lift $ writeBlockMap dev blockMap
  lift $ writeSB dev $ SuperBlock
    { version        = 1
    , devBlockSize   = bdBlockSize dev
    , devBlockCount  = bdNumBlocks dev
    , unmountClean   = True
    , freeBlocks     = finalFree
    , usedBlocks     = initFree - finalFree
    , fileCount      = 0
    , rootDir        = rdirIR
    , blockMapStart  = blockAddrToInodeRef 1
    }

-- | Mounts a filesystem from a given block device.  After this operation
-- completes, the superblock will have its unmountClean flag set to False.
mount :: (HalfsCapable b t r l m) =>
         BlockDevice m
      -> UserID
      -> GroupID
      -> HalfsM b r l m (HalfsState b r l m)
mount dev usr grp = do
  esb <- decode `fmap` lift (bdReadBlock dev 0)
  case esb of
    Left msg -> throwError $ HE_MountFailed $ BadSuperBlock msg
    Right sb -> do
      if unmountClean sb
       then do
         sb' <- lift $ writeSB dev sb{ unmountClean = False }
         HalfsState dev usr grp Nothing   -- No logger, supplied on demand
           `fmap` lift (readBlockMap dev) -- blockmap
           `ap`   newRef sb'              -- superblock 
           `ap`   newLock                 -- filesystem lock
           `ap`   newLockedRscRef 0       -- Locked file node count
           `ap`   newLockedRscRef M.empty -- Locked map: inr -> DH
           `ap`   newLockedRscRef M.empty -- Locked map: inr -> (l, refcnt)
           `ap`   newLockedRscRef M.empty -- Locked map: inr -> (FH, opencnt)
       else throwError $ HE_MountFailed DirtyUnmount

-- | Unmounts the given filesystem.  After this operation completes, the
-- superblock will have its unmountClean flag set to True.
unmount :: (HalfsCapable b t r l m) =>
           HalfsM b r l m ()
unmount = do 
  dhMap <- hasks hsDHMap
  lk    <- hasks hsLock
  dev   <- hasks hsBlockDev
  sbRef <- hasks hsSuperBlock
  
  withLock lk $ withLockedRscRef dhMap $ \dhMapRef -> do 
  -- ^ Grab everything; we do not want to permit other filesystem actions to
  -- occur in other threads during or after teardown. Needs testing. TODO

  sb <- readRef sbRef
  if (unmountClean sb)
   then throwError HE_UnmountFailed
   else do
     -- TODO:
     -- * Persist all dirty data structures (dirents, files w/ buffered IO, etc.)

     -- Sync all directories; clean directory state is a no-op
     mapM_ syncDirectory =<< M.elems `fmap` readRef dhMapRef

     lift $ bdFlush dev
   
     -- Finalize the superblock
     let sb' = sb{ unmountClean = True }
     writeRef sbRef sb'
     lift $ writeSB dev sb'
     return ()
  
fsck :: Int
fsck = undefined


--------------------------------------------------------------------------------
-- Directory manipulation

-- | Makes a directory given an absolute path.
--
-- * Yields HalfsPathComponentNotFound if any path component on the way down to
--   the directory to create does not exist.
--
-- * Yields HalfsAbsolutePathExpected if an absolute path is not provided.
--
mkdir :: (HalfsCapable b t r l m) =>
         FilePath
      -> FileMode
      -> HalfsM b r l m ()
mkdir fp fm = do
  parentIR <- fst `fmap` absPathIR path Directory 
  usr <- getUser
  grp <- getGroup
  makeDirectory parentIR dirName usr grp fm
  return ()
  where
    (path, dirName) = splitFileName fp

rmdir :: (HalfsCapable b t r l m) =>
         FilePath -> HalfsM b r l m ()
rmdir fp = removeDirectory (Just $ takeBaseName fp)
             =<< fst `fmap` absPathIR fp Directory

openDir :: (HalfsCapable b t r l m) =>
           FilePath -> HalfsM b r l m (DirHandle r l)
openDir fp = openDirectory =<< fst `fmap` absPathIR fp Directory

closeDir :: (HalfsCapable b t r l m) =>
            DirHandle r l -> HalfsM b r l m ()
closeDir dh = closeDirectory dh

readDir :: (HalfsCapable b t r l m) =>
           DirHandle r l
        -> HalfsM b r l m [(FilePath, FileStat t)]
readDir dh = do
  withDHLock dh $ do 
    inr      <- getDHINR_lckd dh
    contents <- liftM M.toList $
                  T.mapM (fileStat)
                    =<< fmap deInode `fmap` readRef (dhContents dh)
    thisStat   <- fileStat inr
    parentStat <- fileStat =<< do
      dev <- hasks hsBlockDev
      withLockedInode inr $ do
        p <- inoParent `fmap` drefInode dev inr
        if p == nilInodeRef
         then fmap rootDir . readRef =<< hasks hsSuperBlock
         else return p
    return $ (dotPath, thisStat) : (dotdotPath, parentStat) : contents
 
-- | Synchronize the given directory to disk.
syncDir :: (HalfsCapable b t r l m) =>
           FilePath -> SyncType -> HalfsM b r l m ()
syncDir = undefined

--------------------------------------------------------------------------------
-- File manipulation

-- | Creates a file given an absolute path. Raises HE_ObjectExists if the file
-- already exists.
createFile :: (HalfsCapable b t r l m ) =>
              FilePath
           -> FileMode
           -> HalfsM b r l m ()
createFile fp mode = do
  -- TODO: permissions
  withDir ppath $ \pdh -> do
    findInDir pdh fname RegularFile >>= \rslt ->
      case rslt of
        DF_Found _          -> throwError $ HE_ObjectExists fp
        DF_WrongFileType ft -> throwError $ HE_UnexpectedFileType ft fp
        _                   -> return ()  
    usr  <- getUser
    grp  <- getGroup
    _inr <- F.createFile pdh fname usr grp mode
    return ()
  where
    (ppath, fname) = splitFileName fp
           
-- | Opens a file given an absolute path. Raises HE_FileNotFound if the named
-- file does not exist.  Raises HE_UnexpectedFileType if the given path is not a
-- file. Otherwise, provides a FileHandle to the requested file

-- TODO: modes and flags for open: append, r/w, ronly, truncate, etc., and
-- enforcement of the same
openFile :: (HalfsCapable b t r l m) =>
            FilePath            -- ^ The absolute path of the file
         -> FileOpenFlags       -- ^ open flags / open mode (ronly, wonly, wr)
         -> HalfsM b r l m (FileHandle r l) 
openFile fp oflags = do
  -- TODO: check perms
  logMsg $ "CoreAPI.openFile entry: fp = " ++ show fp ++ ", oflags = " ++ show oflags
  pdh <- openDir ppath
  logMsg $ "CoreAPI.openFile: openDir on parent complete"
  fh  <- findInDir pdh fname RegularFile >>= \rslt ->
           case rslt of
             DF_NotFound         -> throwError $ HE_FileNotFound
             DF_WrongFileType ft -> throwError $ HE_UnexpectedFileType ft fp
             DF_Found (fir, _ft) -> foundFile fir
  logMsg $ "CoreAPI.openFile: findInDir on parent complete"
  closeDir pdh
  logMsg $ "CoreAPI.openFile: closeDir complete"
  return fh
  where
    (ppath, fname) = splitFileName fp
    foundFile      = openFilePrim oflags
                    
read :: (HalfsCapable b t r l m) =>
        FileHandle r l            -- ^ the handle for the open file to read
     -> Word64                    -- ^ the byte offset into the file
     -> Word64                    -- ^ the number of bytes to read
     -> HalfsM b r l m ByteString -- ^ the data read
read fh byteOff len = do
  -- TODO: Check perms
  unless (fhReadable fh) $ HE_BadFileHandleForRead `annErrno` eBADF
  withLock (fhLock fh) $ do 
    inr <- getFHINR_lckd fh
    readStream inr byteOff (Just len)

write :: (HalfsCapable b t r l m) =>
         FileHandle r l     -- ^ the handle for the open file to write
      -> Word64             -- ^ the byte offset into the file
      -> ByteString         -- ^ the data to write
      -> HalfsM b r l m ()
write fh byteOff bytes = do
  -- TODO: Check perms
  unless (fhWritable fh) $ HE_BadFileHandleForWrite `annErrno` eBADF
  withLock (fhLock fh) $ do 
    inr <- getFHINR_lckd fh
    writeStream inr byteOff False bytes

flush :: (HalfsCapable b t r l m) =>
         FileHandle r l -> HalfsM b r l m ()
flush _fh = lift . bdFlush =<< hasks hsBlockDev

syncFile :: (HalfsCapable b t r l m) =>
            FilePath -> SyncType -> HalfsM b r l m ()
syncFile _fp _st = lift . bdFlush =<< hasks hsBlockDev

closeFile :: (HalfsCapable b t r l m) =>
             FileHandle r l -- ^ the handle to the open file to close
          -> HalfsM b r l m ()
closeFile fh = closeFilePrim fh

setFileSize :: (HalfsCapable b t r l m) =>
               FilePath -> Word64 -> HalfsM b r l m ()
setFileSize fp len = 
  withFile fp (fofWriteOnly True) $ \fh -> do
    dev <- hasks hsBlockDev
    bm  <- hasks hsBlockMap
    withLock (fhLock fh) $ do 
      inr <- getFHINR_lckd fh
      let wr  = writeStream_lckd dev bm inr
      withLockedInode inr $ do
        sz <- fsSize `fmap` fileStat_lckd dev inr
        if sz > len
          then wr len True BS.empty                   -- truncate at len
          else wr sz False $ bsReplicate (len - sz) 0 -- pad up to len

setFileTimes :: (HalfsCapable b t r l m) =>
                FilePath            
             -> t                  -- ^ access time
             -> t                  -- ^ modification time  
             -> HalfsM b r l m ()
setFileTimes fp accTm modTm = do
  -- TODO: Check permissions
  modifyInode fp $ \nd -> nd{ inoModifyTime = modTm, inoAccessTime = accTm }

rename :: (HalfsCapable b t r l m) =>
          FilePath -> FilePath -> HalfsM b r l m ()
rename oldFP newFP = do
  -- TODO: perms checks, more errno wrapping
  {- Currently status of unsupported POSIX error behaviors:

     TODO
     ---- 
     [EACCES] A component of either path prefix denies search permission.

     [EACCES] The requested operation requires writing in a directory (e.g.,
              new, new/.., or old/..) whose modes disallow this.

     [EIO] An I/O error occurs while making or updating a directory entry.

     [ELOOP] Too many symbolic links are encountered in translating either
             pathname.  This is taken to be indicative of a looping symbolic
             link.

     [EPERM] The directory containing old is marked sticky, and neither the
             containing directory nor old are owned by the effective user ID.

     [EPERM] The new file exists, the directory containing new is marked sticky,
             and neither the containing directory nor new are owned by the
             effective user ID.

     DEFERRED
     -------- 

     [EXDEV] The link named by new and the file named by old are on different
             logical devices (file systems).  Note that this error code will not
             be returned if the implementation permits cross-device links.

     [EROFS] The requested link requires writing in a directory on a read-only
             file system.

     NOT APPLICABLE (but may be reconsidered later)
     ----------------------------------------------
     [EFAULT] Path points outside the process's allocated address space.

     [EDQUOT] The directory in which the entry for the new name is being placed
              cannot be extended because the user's quota of disk blocks on the
              file system containing the directory has been exhausted.

     [ENAMETOOLONG] A component of a pathname exceeds {NAME_MAX} characters, or
                    an entire path name exceeds {PATH_MAX} characters.
  -}

{- TO support:
     [ENOSPC] The directory in which the entry for the new name is being placed
              cannot be extended because there is no space left on the file
              system containing the directory.
-}

  trace ("oldFP = " ++ oldFP ++ ", newFP = " ++ newFP) $ do

  -- [EINVAL]: An attempt is made to rename `.' or `..'.
  when (oldFP `equalFilePath` dotPath || oldFP `equalFilePath` dotdotPath) $
    HE_RenameFailed `annErrno` eINVAL

  withDirResources $ \oldParentDH newParentDH -> do
    -- Begin critical section over old and new parent directory handles

    mnewDE <- lookupDE newName newParentDH
    oldDE  <- lookupDE oldName oldParentDH
                >>= (`maybe` return)
                      (HE_PathComponentNotFound oldName `annErrno` eNOENT)
                      -- [ENOENT]: A component of the old path DNE
    handleErrors oldDE mnewDE

    let updateContentMaps = do
--          trace ("updateContentMaps: oldDE = " ++ show oldDE) $ do
--          parentContents0 <- readRef (dhContents newParentDH)
          addDirEnt_lckd' True newParentDH oldDE{ deName = newName }
          rmDirEnt_lckd oldParentDH oldName
--          parentContents1 <- readRef (dhContents newParentDH)

--          trace ("new parent contents before = " ++ show parentContents0) $ do
--          trace ("new parent contents after = " ++ show parentContents1) $ do

          b4  <- numFreeBlocks =<< hasks hsBlockMap
          trace ("b4 = " ++ show b4) $ do
          syncDirectory_lckd newParentDH `catchError` \e -> do
            case e of
              -- [ENOSPC]: The directory in which the entry for the new name is
              -- being placed cannot be extended because there is no space left
              -- on the file system containing the directory.
              HE_AllocFailed{} -> e `annErrno` eNOSPC
              _                -> throwError e
          af <- numFreeBlocks =<< hasks hsBlockMap
          trace ("af = " ++ show af) $ do
          return ()  

    trace ("rename: processed errors") $ do

    -- <TODO: comments here about why we're doing things in this particular
    -- order>
    removeNew <- case mnewDE of
      Nothing -> do
        trace ("rename: no dirent for 'new'") $ do
        -- No dirent for 'new', so we can just edit the parent's content map
        updateContentMaps
        return $ return ()
      Just newDE ->
        case deType newDE of
          Directory -> do
            trace ("rename: new is a directory") $ do
            -- New is a directory: ensure that it's empty before updating the
            -- parent's content map
            hbracket (openDirectory $ deInode newDE) closeDirectory $ \newDH ->
              withDHLock newDH $ do
                isEmpty <- M.null `fmap` readRef (dhContents newDH)
                unless isEmpty $ do
                  trace ("rename: new is a non-empty directory, raising error") $ do
                  HE_DirectoryNotEmpty `annErrno` eNOTEMPTY
                updateContentMaps
                return $ removeDirectory Nothing (deInode newDE)
          RegularFile -> do
            -- New is a file
            trace ("rename: new is a file") $ do
            updateContentMaps
            return $ removeFile Nothing (deInode newDE)
          _ -> throwError $ HE_InternalError "rename target type is unsupported"

    removeNew
    trace ("leaving rename") $ return ()
    -- End critical section over old and new parent directory handles
    
  where
    [oldName, newName] = map takeFileName [oldFP, newFP]
    [oldPP, newPP]     = map takeDirectory [oldFP, newFP]
    --
    withDirResources f =
      -- bracket & lock the old and new parent directories, being careful not to
      -- double-lock when they're the same.
      withDir' openDir' oldPP $ \opdh -> 
        withDir' openDir' newPP $ \npdh ->
          (if oldPP `equalFilePath` newPP
            then withDHLock opdh
            else withDHLock opdh . withDHLock npdh
          ) $ f opdh npdh
    -- 
    openDir' dp = openDir dp `catchError` \e ->
      case e of
        -- [ENOTDIR]: A component of path prefix is not a directory
        HE_UnexpectedFileType{}    -> e `annErrno` eNOTDIR
        -- [ENOENT] A component of path does not exist
        HE_PathComponentNotFound{} -> e `annErrno` eNOENT
        _                          -> throwError e
    --
    handleErrors oldDE mnewDE = do
      -- [EINVAL]: Old is a parent directory of new
      when (deType oldDE == Directory && oldFP `isParentOf` newFP) $
        HE_RenameFailed `annErrno` eINVAL

      case mnewDE of
        Nothing    -> return ()
        Just newDE -> do

          -- [EISDIR]: new is a directory, but old is not a directory.
          when (nft == Directory && oft /= Directory) $
            HE_RenameFailed `annErrno` eISDIR

          -- [ENOTDIR]: old is a directory, but new is not a directory.
          when (oft == Directory && nft /= Directory) $
            HE_RenameFailed `annErrno` eNOTDIR

          where
            nft = deType newDE
            oft = deType oldDE
    -- 
    p1 `isParentOf` p2 = length l1 < length l2 && and (zipWith (==) l1 l2)
                         where l1 = splitDirectories p1
                               l2 = splitDirectories p2
      

--------------------------------------------------------------------------------
-- Access control

chmod :: (HalfsCapable b t r l m) =>
         FilePath -> FileMode -> HalfsM b r l m ()
chmod fp mode = do
  -- TODO: Check perms
  modifyInode fp $ \nd -> nd{ inoMode = mode }

chown :: (HalfsCapable b t r l m) =>
         FilePath
      -> Maybe UserID  -- ^ Nothing indicates no change to user
      -> Maybe GroupID -- ^ Nothing indicates no change to group
      -> HalfsM b r l m ()
chown fp musr mgrp = do
  -- TODO: Check perms
  modifyInode fp $ \nd ->
    nd{ inoUser  = maybe (inoUser nd) id musr
      , inoGroup = maybe (inoGroup nd) id mgrp
      }

access :: (HalfsCapable b t r l m) =>
          FilePath -> [AccessRight] -> HalfsM b r l m ()
access = undefined


--------------------------------------------------------------------------------
-- Link manipulation

-- | A POSIX link(2)-like operation: makes a hard file link.
mklink :: (HalfsCapable b t r l m) =>
          FilePath -> FilePath -> HalfsM b r l m ()
mklink path1 {-src-} path2 {-dst-} = do
  {- Currently status of unsupported POSIX error behaviors:

     TODO
     ---- 

     [EACCES] A component of either path prefix denies search permission.
     [EACCES] The requested link requires writing in a directory with a mode
              that denies write permission.
     [EACCES] The current process cannot access the existing file.

     [EIO]    An I/O error occurs while reading from or writing to the file 
              system to make the directory entry.

     [ELOOP] Too many symbolic links are encountered in translating one of the
             pathnames.  This is taken to be indicative of a looping symbolic
             link.

     DEFERRED
     -------- 

     [EROFS] The requested link requires writing in a directory on a read-only
             file system.

     [EXDEV] The link named by path2 and the file named by path1 are on
             different file systems.

     NOT APPLICABLE (but may be reconsidered later)
     ----------------------------------------------

     [EFAULT]       One of the pathnames specified is outside the process's
                    allocated address space.

     [EDQUOT]       The directory in which the entry for the new link is being
                    placed cannot be extended because the user's quota of disk
                    blocks on the file system containing the directory has been
                    exhausted.

     [EMLINK]       The file already has {LINK_MAX} links.

     [ENAMETOOLONG] A component of a pathname exceeds {NAME_MAX} characters, or
                    an entire path name exceeded {PATH_MAX} characters.
  -}

  hbracket openSrcFile closeFile $ \p1fh -> do     
    hbracket openDstDir closeDir $ \p2dh -> do
      withLock (fhLock p1fh) $ do 
        srcINR <- getFHINR_lckd p1fh
        addLink p2dh srcINR
        incLinkCount srcINR
  where
    addLink p2dh inr = do
      let fname     = takeFileName path2               
          linkPerms = FileMode [Read,Write] [Read] [Read]
          -- TODO: Obtain the proper permissions for the link
      usr <- getUser
      grp <- getGroup
      withDHLock p2dh $ do 
        addDirEnt_lckd p2dh fname inr usr grp linkPerms RegularFile
          `catchError` \e -> do
            case e of
              -- [EEXIST]: The link named by path2 already exists.
              HE_ObjectExists{} -> e `annErrno` eEXIST
              _                 -> throwError e
        -- Manual directory sync here in order to catch and wrap errors
        syncDirectory_lckd p2dh `catchError` \e -> do
          rmDirEnt_lckd p2dh fname
          case e of
            -- [ENOSPC]: The directory in which the entry for the new link is
            -- being placed cannot be extended because there is no space left
            -- on the file system containing the directory.
            HE_AllocFailed{} -> e `annErrno` eNOSPC
            _                -> throwError e
    -- 
    openDstDir = openDir (takeDirectory path2) `catchError` \e ->
      case e of
        -- [ENOTDIR]: A component of path2's pfx is not a directory
        HE_UnexpectedFileType{}    -> e `annErrno` eNOTDIR
        -- [ENOENT] A component of path2's pfx does not exist
        HE_PathComponentNotFound{} -> e `annErrno` eNOENT
        _                       -> throwError e
    -- 
    openSrcFile = openFile path1 fofReadOnly `catchError` \e ->
      case e of
        -- [EPERM]: The file named by path1 is a directory
        HE_UnexpectedFileType Directory _ -> e `annErrno` ePERM
        -- [ENOTDIR]: A component of path1's pfx is not a directory
        HE_UnexpectedFileType _ _         -> e `annErrno` eNOTDIR
        -- [ENOENT] A component of path1's pfx does not exist
        HE_PathComponentNotFound _        -> e `annErrno` eNOENT
        -- [ENOENT] The file named by path1 does not exist
        HE_FileNotFound                   -> e `annErrno` eNOENT
        _                                 -> throwError e

rmlink :: (HalfsCapable b t r l m) =>
          FilePath -> HalfsM b r l m ()
rmlink fp = removeFile (Just $ takeFileName fp)
              =<< fst `fmap` absPathIR fp RegularFile

createSymLink :: (HalfsCapable b t r l m) =>
                 FilePath -> FilePath -> HalfsM b r l m ()
createSymLink = undefined

readSymLink :: (HalfsCapable b t r l m) =>
               FilePath -> HalfsM b r l m FilePath
readSymLink = undefined


--------------------------------------------------------------------------------
-- Filesystem stats

fstat :: (HalfsCapable b t r l m) =>
         FilePath -> HalfsM b r l m (FileStat t)
fstat fp = fileStat =<< fst `fmap` absPathIR fp AnyFileType

fsstat :: (HalfsCapable b t r l m) =>
          HalfsM b r l m FileSystemStats
fsstat = do
  dev         <- hasks hsBlockDev
  numNodesRsc <- hasks hsNumFileNodes
  bm          <- hasks hsBlockMap 
  fileCnt     <- fromIntegral `fmap` withLockedRscRef numNodesRsc readRef
  freeCnt     <- fromIntegral `fmap` numFreeBlocks bm
  return FSS
    { fssBlockSize   = fromIntegral $ bdBlockSize dev
    , fssBlockCount  = fromIntegral $ bdNumBlocks dev
    , fssBlocksFree  = freeCnt
    , fssBlocksAvail = freeCnt -- TODO: blocks avail to non-root
    , fssFileCount   = fileCnt
    , fssFilesFree   = 0       -- TODO: need to supply free inode count
    , fssFilesAvail  = 0       -- TODO inodes avail to non-root
    }


--------------------------------------------------------------------------------
-- Utility functions & consts

-- | Atomic inode modification wrapper
modifyInode :: HalfsCapable b t r l m =>
               FilePath
            -> (Inode t -> Inode t)
            -> HalfsM b r l m ()
modifyInode fp f = 
  withFile fp (fofReadWrite True) $ \fh ->
    withLock (fhLock fh) $
      getFHINR_lckd fh >>= flip atomicModifyInode (return . f)

-- | Find the InodeRef corresponding to the given path.  On error, raises
-- exceptions HE_PathComponentNotFound, HE_AbsolutePathExpected, or
-- HE_UnexpectedFileType.
absPathIR :: HalfsCapable b t r l m =>
             FilePath
          -> FileType
          -> HalfsM b r l m (InodeRef, FileType)
absPathIR fp ftype = do
  if isAbsolute fp
   then do
     sbRef  <- hasks hsSuperBlock 
     rdirIR <- rootDir `fmap` readRef sbRef
     mir    <- find rdirIR ftype (drop 1 $ splitDirectories fp)
     case mir of
       DF_NotFound         -> throwError $ HE_PathComponentNotFound fp
       DF_WrongFileType ft -> throwError $ HE_UnexpectedFileType ft fp
       DF_Found (ir, ft)   -> return (ir, ft)
   else
     throwError HE_AbsolutePathExpected

-- | Bracketed directory open
withDir :: HalfsCapable b t r l m =>
           FilePath
        -> (DirHandle r l -> HalfsM b r l m a)
        -> HalfsM b r l m a
withDir = withDir' openDir

-- | Bracketed directory open, parameterized by a function for opening a dir
withDir' :: HalfsCapable b t r l m =>
            (FilePath -> HalfsM b r l m (DirHandle r l))
         -> FilePath
         -> (DirHandle r l -> HalfsM b r l m a)
         -> HalfsM b r l m a
withDir' openF = (`hbracket` closeDir) . openF

withFile :: (HalfsCapable b t r l m) =>
            FilePath
         -> FileOpenFlags
         -> (FileHandle r l -> HalfsM b r l m a)
         -> HalfsM b r l m a
withFile fp oflags = 
  hbracket (openFile fp oflags) closeFile

writeSB :: (HalfsCapable b t r l m) =>
           BlockDevice m -> SuperBlock -> m SuperBlock
writeSB dev sb = do 
  let sbdata = encode sb
  assert (BS.length sbdata <= fromIntegral (bdBlockSize dev)) $ return ()
  bdWriteBlock dev 0 sbdata
  bdFlush dev
  return sb

getUser :: HalfsCapable b t r l m =>
           HalfsM b r l m UserID
getUser = hasks hsUserID

getGroup :: HalfsCapable b t r l m =>
            HalfsM b r l m GroupID
getGroup = hasks hsGroupID
