{-# LANGUAGE FlexibleContexts, ScopedTypeVariables #-}
module Halfs.CoreAPI where

import Control.Exception (assert)
import Data.ByteString   (ByteString)
import qualified Data.ByteString  as BS
import qualified Data.Map         as M
import Data.Serialize
import qualified Data.Traversable as T
import Data.Word
import Foreign.C.Error
import System.FilePath

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Directory
import Halfs.Errors
import Halfs.File
import Halfs.HalfsState
import Halfs.Inode
import Halfs.Monad
import Halfs.Protection
import Halfs.SuperBlock
import Halfs.Types

import System.Device.BlockDevice

import Debug.Trace

data SyncType = Data | Everything

data FileSystemStats = FSS {
    fssBlockSize     :: Integer
  , fssBlockCount    :: Integer
  , fssBlocksFree    :: Integer
  , fssBlocksAvail   :: Integer
  , fssFileCount     :: Integer
  , fssMaxNameLength :: Integer
  }


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
         BlockDevice m -> HalfsM m SuperBlock
newfs dev = do
  when (superBlockSize > bdBlockSize dev) $
    fail "The device's block size is insufficiently large!"

  blockMap <- lift $ newBlockMap dev
  numFree  <- readRef (bmNumFree blockMap)
  rdirAddr <- lift (alloc1 blockMap) >>=
              maybe (fail "Unable to allocate block for rdir inode") return

  -- Build the root directory inode and persist it; note that we do not use
  -- Directory.makeDirectory here because this is a special case where we have
  -- no parent directory.

  let rdirInode = blockAddrToInodeRef rdirAddr
  dirInode <- lift $
    buildEmptyInodeEnc 
      dev
      Directory
      rootDirPerms
      -- ^ TODO: Should we have the caller provide root dir perms?
      rdirInode
      nilInodeRef
      rootUser
      rootGroup
  assert (BS.length dirInode == fromIntegral (bdBlockSize dev)) $ do
  lift $ bdWriteBlock dev rdirAddr dirInode

  -- Persist the remaining data structures
  lift $ writeBlockMap dev blockMap
  lift $ writeSB       dev $ superBlock rdirInode (numFree - rdirBlks) rdirBlks
 where
   rdirBlks                = 1
   superBlock rdirIR nf nu = SuperBlock {
     version        = 1
   , devBlockSize   = bdBlockSize dev
   , devBlockCount  = bdNumBlocks dev
   , unmountClean   = True
   , freeBlocks     = nf 
   , usedBlocks     = nu
   , fileCount      = 0
   , rootDir        = rdirIR
   , blockMapStart  = blockAddrToInodeRef 1
   }

-- | Mounts a filesystem from a given block device.  After this operation
-- completes, the superblock will have its unmountClean flag set to False.
mount :: (HalfsCapable b t r l m) =>
         BlockDevice m -> HalfsM m (HalfsState b r l m)
mount dev = do
  esb <- decode `fmap` lift (bdReadBlock dev 0)
  case esb of
    Left msg -> throwError $ HE_MountFailed $ BadSuperBlock msg
    Right sb -> do
      if unmountClean sb
       then do
         sb' <- lift $ writeSB dev sb{ unmountClean = False }
         HalfsState dev
           `fmap` lift (readBlockMap dev) -- blockmap
           `ap`   newRef sb'              -- superblock 
           `ap`   newLock                 -- filesystem lock
           `ap`   newLockedRscRef M.empty -- Locked map: InodeRef -> DirHandle
           `ap`   newLockedRscRef M.empty -- Locked map: InodeRef -> (l, refcnt)
       else throwError $ HE_MountFailed DirtyUnmount

-- | Unmounts the given filesystem.  After this operation completes, the
-- superblock will have its unmountClean flag set to True.
unmount :: (HalfsCapable b t r l m) =>
           HalfsState b r l m -> HalfsM m ()
unmount fs@HalfsState{hsBlockDev = dev, hsSuperBlock = sbRef} = 
  withLock (hsLock fs)          $ do 
  withLockedRscRef (hsDHMap fs) $ \dhMapRef -> do 
  -- ^ Grab everything; we do not want to permit other filesystem actions to
  -- occur in other threads during or after teardown. Needs testing. TODO

  sb <- readRef sbRef
  if (unmountClean sb)
   then throwError HE_UnmountFailed
   else do
     -- TODO:
     -- * Persist all dirty data structures (dirents, files w/ buffered IO, etc.)

     -- Sync all directories; clean directory state is a no-op
     mapM_ (syncDirectory fs) =<< M.elems `fmap` readRef dhMapRef

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
         HalfsState b r l m
      -> FileMode
      -> FilePath
      -> HalfsM m ()
mkdir fs fm fp = do
  parentIR <- absPathIR fs path Directory 
  u <- getUser
  g <- getGroup
  makeDirectory fs parentIR dirName u g fm
  return ()
  where
    (path, dirName) = splitFileName fp

rmdir :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FilePath -> HalfsM m ()
rmdir = undefined

openDir :: (HalfsCapable b t r l m) =>
           HalfsState b r l m -> FilePath -> HalfsM m (DirHandle r l)
openDir fs fp = absPathIR fs fp Directory >>= openDirectory fs

closeDir :: (HalfsCapable b t r l m) =>
            HalfsState b r l m -> DirHandle r l -> HalfsM m ()
closeDir fs dh = closeDirectory fs dh

readDir :: (HalfsCapable b t r l m) =>
           HalfsState b r l m
        -> DirHandle r l
        -> HalfsM m [(FilePath, FileStat t)]
readDir fs dh =
  liftM M.toList $
    T.mapM (fileStat fs . deInode)
      =<< withLock (dhLock dh) (readRef $ dhContents dh)

-- | Synchronize the given directory to disk.
syncDir :: (HalfsCapable b t r l m) =>
           HalfsState b r l m -> FilePath -> SyncType -> HalfsM m ()
syncDir = undefined

--------------------------------------------------------------------------------
-- File manipulation

-- | Opens a file given an absolute path.
--
-- * Yields HalfsPathComponentNotFound if any path component on the way down to
-- the file does not exist.
--
-- * Yields HalfsAbsolutePathExpected if an absolute path is not provided
--
-- * Yields HalfsObjectNotFound if the request file does not exist and create is
-- false
--
-- * Otherwise, provides a FileHandle to the requested file

-- TODO: modes and flags for open: append, r/w, ronly, truncate, etc., and
-- enforcement of the same
openFile :: (HalfsCapable b t r l m) =>
            HalfsState b r l m              -- ^ The FS
         -> FilePath                        -- ^ The absolute path of the file
         -> Bool                            -- ^ Should we create the file if it is not
                                            --   found?
         -> HalfsM m FileHandle 
openFile fs fp creat = do
  pdh <- openDir fs path
  fh  <- findInDir pdh fname RegularFile >>= \rslt ->
           case rslt of
             DF_NotFound         -> noFile pdh
             DF_WrongFileType ft -> throwError $ HE_UnexpectedFileType ft fp
             DF_Found fir        -> foundFile fir
  closeDir fs pdh
  return fh
  where
    (path, fname) = splitFileName fp
    -- 
    noFile parentDH =
      if creat
       then do
         usr <- getUser
         grp <- getGroup
         fir <- createFile
                  fs
                  parentDH
                  fname
                  usr
                  grp
                  defaultFilePerms -- FIXME
         fh <- openFilePrim fir
         -- TODO/FIXME: mark this FH as open in FD structures &
         -- r/w'able perms etc.?
         return fh
       else do
          throwError $ HE_FileNotFound
    --
    foundFile fir = do
      if creat
       then do
         throwError $ HE_ObjectExists fp
       else do
         fh <- openFilePrim fir
         -- TODO/FIXME: mark this FH as open in FD structures, store
         -- r/w'able perms etc.?
         return fh
                    
read :: (HalfsCapable b t r l m) =>
        HalfsState b r l m  -- ^ the filesystem
     -> FileHandle          -- ^ the handle for the open file to read
     -> Word64              -- ^ the byte offset into the file
     -> Word64              -- ^ the number of bytes to read
     -> HalfsM m ByteString -- ^ the data read
read fs fh byteOff len = do
  -- TODO: check fh modes & perms (e.g., write only etc)
  readStream fs (fhInode fh) byteOff (Just len)

write :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -- ^ the filesystem
      -> FileHandle         -- ^ the handle for the open file to write
      -> Word64             -- ^ the byte offset into the file
      -> ByteString         -- ^ the data to write
      -> HalfsM m ()
write fs fh byteOff bytes = 
  -- TODO: check fh modes & perms (e.g., read only, not owner, etc)
  writeStream fs (fhInode fh) byteOff False bytes

flush :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FileHandle -> HalfsM m ()
flush = undefined

syncFile :: (HalfsCapable b t r l m) =>
            HalfsState b r l m -> FilePath -> SyncType ->HalfsM m ()
syncFile = undefined

closeFile :: (HalfsCapable b t r l m) =>
             HalfsState b r l m -- ^ the filesystem
          -> FileHandle         -- ^ the handle to the open file to close
          -> HalfsM m ()
closeFile _fs _fh = do
  -- TODO/FIXME: sync, mark fh closed in FD structures
  return ()

setFileSize :: (HalfsCapable b t r l m) =>
               HalfsState b r l m -> FilePath -> Word64 -> HalfsM m ()
setFileSize = undefined

setFileTimes :: (HalfsCapable b t r l m) =>
                HalfsState b r l m -> FilePath -> t -> t -> HalfsM m ()
setFileTimes = undefined

rename :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> FilePath -> FilePath -> HalfsM m ()
rename = undefined


--------------------------------------------------------------------------------
-- Access control

chmod :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FilePath -> FileMode -> HalfsM m ()
chmod = undefined

chown :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FilePath -> UserID -> GroupID -> HalfsM m ()
chown = undefined

-- | JS XXX/TODO: What's the intent of this function?
access :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> FilePath -> [AccessRight] -> HalfsM m ()
access = undefined


--------------------------------------------------------------------------------
-- Link manipulation

-- | A POSIX link(2)-like operation: makes a hard file link.
mklink :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> FilePath -> FilePath -> HalfsM m ()
mklink fs path1 {-src-} path2 {-dst-} = do

  {- Currently does not implement the following POSIX error behaviors:

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

  -- Try to open path1 (the source file)
  p1h <- openFile fs path1 False
           `catchError` \e ->
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

  -- Try to open path2's path prefix
  let (p2pfx, p2fname) = splitFileName path2
  p2pfxdh <- openDir fs p2pfx
               `catchError` \e ->
                 case e of
                   -- [ENOTDIR]: A component of path2's pfx is not a directory
                   HE_UnexpectedFileType{}    -> e `annErrno` eNOTDIR
                   -- [ENOENT] A component of path2's pfx does not exist
                   HE_PathComponentNotFound{} -> e `annErrno` eNOENT
                   _                       -> throwError e
  
  usr <- getUser
  grp <- getGroup
  let srcINR  = fhInode p1h
      cleanup = decLinkCount fs srcINR

  incLinkCount fs srcINR

  addDirEnt p2pfxdh p2fname srcINR usr grp defaultFilePerms RegularFile
    `catchError` \e -> do
      cleanup
      case e of
        -- [EEXIST]: The link named by path2 already exists.
        HE_ObjectExists{} -> e `annErrno` eEXIST
        _                 -> throwError e

  -- TODO: If the syncDirectory belows fails, we'll need to remove the new
  -- DirEnt from the map.  This kind of rollback-on-exception will be common,
  -- and must maintain thread safety properties, so we probably want to think of
  -- a clean way to handle it.

  syncDirectory fs p2pfxdh
    `catchError` \e -> do
      cleanup
      case e of
        -- [ENOSPC]: The directory in which the entry for the new link is being
        -- placed cannot be extended because there is no space left on the file
        -- system containing the directory.
        HE_AllocFailed{} -> e `annErrno` eNOSPC
        _                -> throwError e
          
  closeDir fs p2pfxdh
  where
    e `annErrno` errno = throwError (e `HE_ErrnoAnnotated` errno)

rmlink :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> FilePath -> HalfsM m ()
rmlink = undefined

createSymLink :: (HalfsCapable b t r l m) =>
                 HalfsState b r l m -> FilePath -> FilePath -> HalfsM m ()
createSymLink = undefined

readSymLink :: (HalfsCapable b t r l m) =>
               HalfsState b r l m -> FilePath -> HalfsM m FilePath
readSymLink = undefined


--------------------------------------------------------------------------------
-- Filesystem stats

fstat :: (HalfsCapable b t r l m) =>
         HalfsState b r l m -> FilePath -> HalfsM m (FileStat t)
fstat fs fp = absPathIR fs fp AnyFileType >>= fileStat fs

fsstat :: (HalfsCapable b t r l m) =>
          HalfsState b r l m -> HalfsM m FileSystemStats
fsstat = undefined


--------------------------------------------------------------------------------
-- Utility functions

-- | Find the InodeRef corresponding to the given path.  On error, raises
-- exceptions HE_PathComponentNotFound, HE_AbsolutePathExpected, or
-- HE_UnexpectedFileType.
absPathIR :: HalfsCapable b t r l m =>
             HalfsState b r l m
          -> FilePath
          -> FileType
          -> HalfsM m InodeRef
absPathIR fs fp ftype = do
  if isAbsolute fp
   then do
     rdirIR <- rootDir `fmap` readRef (hsSuperBlock fs)
     mir    <- find fs rdirIR ftype (drop 1 $ splitDirectories fp)
     case mir of
       DF_NotFound         -> throwError $ HE_PathComponentNotFound fp
       DF_WrongFileType ft -> throwError $ HE_UnexpectedFileType ft fp
       DF_Found ir         -> return ir
   else
     throwError HE_AbsolutePathExpected

withDir :: HalfsCapable b t r l m =>
           HalfsState b r l m
        -> FilePath
        -> (DirHandle r l -> HalfsM m a)
        -> HalfsM m a
withDir fs fp f = do
  dh   <- openDir fs fp
  rslt <- f dh `catchError` \e -> closeDir fs dh >> throwError e
  closeDir fs dh
  return rslt

fsElemExists :: HalfsCapable b t r l m =>
                HalfsState b r l m
             -> FilePath
             -> FileType
             -> HalfsM m Bool
fsElemExists = undefined

writeSB :: (HalfsCapable b t r l m) =>
           BlockDevice m -> SuperBlock -> m SuperBlock
writeSB dev sb = do 
  let sbdata = encode sb
  assert (BS.length sbdata <= fromIntegral (bdBlockSize dev)) $ return ()
  bdWriteBlock dev 0 sbdata
  bdFlush dev
  return sb

-- TODO: Placeholder
getUser :: Monad m => m UserID
getUser = return rootUser

-- TODO: Placeholder
getGroup :: Monad m => m GroupID
getGroup = return rootGroup

-- TODO: Placeholder, these need to come from the execution environment
rootDirPerms :: FileMode
rootDirPerms = FileMode [Read,Write,Execute] [] []

-- TODO: Placeholder, these need to come from the execution environment
defaultDirPerms :: FileMode
defaultDirPerms = FileMode [Read,Write,Execute] [Read, Execute] [Read, Execute]

-- TODO: Placeholder, these need to come from the execution environment
defaultFilePerms :: FileMode
defaultFilePerms = FileMode [Read,Write] [Read] [Read]

