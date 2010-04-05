{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Command line tool for mounting a halfs filesystem from userspace via
-- hFUSE/FUSE.

module Main
where

import Control.Applicative
import Control.Exception     (assert)
import Data.Array.IO         (IOUArray)
import Data.Bits
import Data.IORef            (IORef)
import Data.Word             
import System.Console.GetOpt 
import System.Directory      (doesFileExist)
import System.Environment
import System.IO hiding      (openFile)
import System.Posix.Types    ( ByteCount
                             , DeviceID
                             , EpochTime
                             , FileMode
                             , FileOffset
                             , GroupID
                             , UserID
                             )
import System.Posix.User     (getRealUserID, getRealGroupID)
import System.Fuse     

import Halfs.Classes
import Halfs.CoreAPI
import Halfs.File (FileHandle, openFilePrim)
import Halfs.Errors
import Halfs.HalfsState
import Halfs.Monad
import Halfs.MonadUtils
import Halfs.Utils
import System.Device.BlockDevice
import System.Device.File
import System.Device.Memory

import qualified Data.ByteString  as BS
import qualified Data.Map         as M
import qualified Halfs.Protection as HP
import qualified Halfs.Types      as H
import qualified Tests.Utils      as TU
import qualified Prelude
import Prelude hiding (catch, log, read)

-- import Debug.Trace

-- Halfs-specific stuff we carry around in our FUSE functions; note that the
-- FUSE library does this via opaque ptr to user data, but since hFUSE
-- reimplements fuse_main and doesn't seem to provide a way to hang onto
-- private data, we just carry the data ourselves.

type Logger m              = String -> m ()
data HalfsSpecific b r l m = HS {
    hspLogger  :: Logger m
  , hspState   :: HalfsState b r l m
    -- ^ The filesystem state, as provided by CoreAPI.mount.
  , hspFpdhMap :: H.LockedRscRef l r (M.Map FilePath (H.DirHandle r l))
    -- ^ Tracks DirHandles across halfs{Open,Read,Release}Directory invocations;
    -- we should be able to do this via HFuse and the fuse_file_info* out
    -- parameter for the 'open' fuse operation, but the binding doesn't support
    -- this.  However, we'd probably still need something persistent here to go
    -- from the opaque handle we stored in the fuse_file_info struct to one of
    -- our DirHandles.
  }

-- This isn't a halfs limit, but we have to pick something for FUSE.
maxNameLength :: Integer
maxNameLength = 32768

main :: IO ()
main = do
  (opts, argv1) <- do
    argv0 <- getArgs
    case getOpt RequireOrder options argv0 of
      (o, n, [])   -> return (foldl (flip ($)) defOpts o, n)
      (_, _, errs) -> ioError $ userError $ concat errs ++ usageInfo hdr options
        where hdr = "Usage: halfs [OPTION...] <FUSE CMDLINE>"

  let sz = optSecSize opts; n = optNumSecs opts
  (exists, dev) <- maybe (fail "Unable to create device") return
    =<<
    let wb = (liftM . liftM) . (,) in
    if optMemDev opts
     then wb False $ newMemoryBlockDevice n sz <* putStrLn "Created new memdev."
     else case optFileDev opts of
            Nothing -> fail "Can't happen"
            Just fp -> do
              exists <- doesFileExist fp
              wb exists $ 
                if exists
                 then newFileBlockDevice fp sz 
                        <* putStrLn "Created filedev from existing file."
                 else TU.withFileStore False fp sz n (`newFileBlockDevice` sz)
                        <* putStrLn "Created filedev from new file."  

  uid <- (HP.UID . fromIntegral) `fmap` getRealUserID
  gid <- (HP.GID . fromIntegral) `fmap` getRealGroupID

  when (not exists) $ do
    execNoEnv $ newfs dev uid gid $
      H.FileMode
        [H.Read, H.Write, H.Execute]
        [H.Read,          H.Execute]
        [                          ]
    return ()

  fs <- execNoEnv $ mount dev uid gid

  System.IO.withFile (optLogFile opts) WriteMode $ \h -> do

  let log s = hPutStrLn h s >> hFlush h
  dhMap <- newLockedRscRef M.empty
  withArgs argv1 $ fuseMain (ops (HS log fs dhMap)) $ \e -> do
    log $ "*** Exception: " ++ show e
    return eFAULT

--------------------------------------------------------------------------------
-- Halfs-hFUSE filesystem operation implementation

-- JS: ST monad impls will have to get mapped to hFUSE ops via stToIO?

ops :: HalfsSpecific (IOUArray Word64 Bool) IORef IOLock IO
    -> FuseOperations (FileHandle IORef IOLock)
ops hsp = FuseOperations
  { fuseGetFileStat          = halfsGetFileStat        hsp
  , fuseReadSymbolicLink     = halfsReadSymbolicLink   hsp
  , fuseCreateDevice         = halfsCreateDevice       hsp
  , fuseCreateDirectory      = halfsCreateDirectory    hsp
  , fuseRemoveLink           = halfsRemoveLink         hsp
  , fuseRemoveDirectory      = halfsRemoveDirectory    hsp
  , fuseCreateSymbolicLink   = halfsCreateSymbolicLink hsp
  , fuseRename               = halfsRename             hsp
  , fuseCreateLink           = halfsCreateLink         hsp
  , fuseSetFileMode          = halfsSetFileMode        hsp
  , fuseSetOwnerAndGroup     = halfsSetOwnerAndGroup   hsp
  , fuseSetFileSize          = halfsSetFileSize        hsp
  , fuseSetFileTimes         = halfsSetFileTimes       hsp
  , fuseOpen                 = halfsOpen               hsp
  , fuseRead                 = halfsRead               hsp
  , fuseWrite                = halfsWrite              hsp
  , fuseGetFileSystemStats   = halfsGetFileSystemStats hsp
  , fuseFlush                = halfsFlush              hsp
  , fuseRelease              = halfsRelease            hsp
  , fuseSynchronizeFile      = halfsSyncFile           hsp
  , fuseOpenDirectory        = halfsOpenDirectory      hsp
  , fuseReadDirectory        = halfsReadDirectory      hsp
  , fuseReleaseDirectory     = halfsReleaseDirectory   hsp
  , fuseSynchronizeDirectory = halfsSyncDirectory      hsp
  , fuseAccess               = halfsAccess             hsp
  , fuseInit                 = halfsInit               hsp
  , fuseDestroy              = halfsDestroy            hsp
  }

halfsGetFileStat :: HalfsCapable b t r l m =>
                    HalfsSpecific b r l m
                 -> FilePath
                 -> m (Either Errno FileStat)
halfsGetFileStat hsp@HS{ hspLogger = log } fp = do
  log $ "halfsGetFileStat: fp = " ++ show fp
  eestat <- execOrErrno hsp eINVAL id (fstat fp)
  case eestat of
    Left en -> do
      log $ "  (fstat failed w/ " ++ show en ++ ")"
      return $ Left en
    Right stat -> 
      Right `fmap` hfstat2fstat stat

halfsReadSymbolicLink :: HalfsCapable b t r l m =>
                         HalfsSpecific b r l m
                      -> FilePath
                      -> m (Either Errno FilePath)
halfsReadSymbolicLink HS{ hspLogger = _log, hspState = _fs } _fp = do 
  error "halfsReadSymbolicLink: Not Yet Implemented" -- TODO
  return (Left eNOSYS)

halfsCreateDevice :: HalfsCapable b t r l m =>
                     HalfsSpecific b r l m
                  -> FilePath -> EntryType -> FileMode -> DeviceID
                  -> m Errno
halfsCreateDevice hsp@HS{ hspLogger = log } fp etype mode _devID = do
  log $ "halfsCreateDevice: fp = " ++ show fp ++ ", etype = " ++ show etype ++
        ", mode = " ++ show mode
  case etype of
    RegularFile -> do
      log $ "halfsCreateDevice: Regular file w/ " ++ show hmode
      execDefault hsp $ hlocal (withLogger log) $ createFile fp hmode
    _ -> do
      log $ "halfsCreateDevice: Error: Unsupported EntryType encountered."
      return eINVAL
  where hmode = mode2hmode mode

halfsCreateDirectory :: HalfsCapable b t r l m =>
                        HalfsSpecific b r l m
                     -> FilePath -> FileMode
                     -> m Errno
halfsCreateDirectory hsp@HS{ hspLogger = log } fp mode = do
  log $ "halfsCreateDirectory: fp = " ++ show fp
  execDefault hsp $ mkdir fp (mode2hmode mode)
         
halfsRemoveLink :: HalfsCapable b t r l m =>
                   HalfsSpecific b r l m
                -> FilePath
                -> m Errno
halfsRemoveLink HS{ hspLogger = _log, hspState = _fs } _fp = do
  error "halfsRemoveLink: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsRemoveDirectory :: HalfsCapable b t r l m =>
                        HalfsSpecific b r l m
                     -> FilePath
                     -> m Errno
halfsRemoveDirectory hsp@HS{ hspLogger = log } fp = do
  log $ "halfsRemoveDirectory: removing " ++ show fp
  execDefault hsp $ rmdir fp

         
halfsCreateSymbolicLink :: HalfsCapable b t r l m =>
                           HalfsSpecific b r l m
                        -> FilePath -> FilePath
                        -> m Errno
halfsCreateSymbolicLink HS{ hspLogger = _log, hspState = _fs } _src _dst = do
  error $ "halfsCreateSymbolicLink: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsRename :: HalfsCapable b t r l m =>
               HalfsSpecific b r l m
            -> FilePath -> FilePath
            -> m Errno
halfsRename HS{ hspLogger = _log, hspState = _fs } _old _new = do
  error $ "halfsRename: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsCreateLink :: HalfsCapable b t r l m =>
                   HalfsSpecific b r l m
                -> FilePath -> FilePath
                -> m Errno
halfsCreateLink hsp@HS{ hspLogger = log } src dst = do
  log $ "halfsCreateLink: creating hard link from '" ++ src
        ++ "' to '" ++ dst ++ "'"
  execDefault hsp $ mklink src dst
         
halfsSetFileMode :: HalfsCapable b t r l m =>
                    HalfsSpecific b r l m
                 -> FilePath -> FileMode
                 -> m Errno
halfsSetFileMode hsp@HS{ hspLogger = log } fp mode = do
  log $ "halfsSetFileMode: setting " ++ show fp ++ " to mode " ++ show mode
  execDefault hsp $ chmod fp (mode2hmode mode)
         
halfsSetOwnerAndGroup :: HalfsCapable b t r l m =>
                         HalfsSpecific b r l m
                      -> FilePath -> UserID -> GroupID
                      -> m Errno
halfsSetOwnerAndGroup hsp@HS{ hspLogger = log } fp uid' gid' = do
  -- uid and gid get passed as System.Posix.Types.C[UG]id which are newtype'd
  -- Word32s.  Unfortunately, this means that a user or group argument of -1
  -- (which means "unchanged" according to the man page for chown(2)) is only
  -- visible to us here as maxBound :: Word32.

  let uid   = cvt uid'
      gid   = cvt gid'
      cvt x = if fromIntegral x == (maxBound :: Word32)
               then Nothing
               else Just (fromIntegral x)

  log $ "halfsSetOwnerAndGroup: setting " ++ show fp ++ " to user = "
        ++ show uid ++ ", group = " ++ show gid
  execDefault hsp $ chown fp uid gid
         
halfsSetFileSize :: HalfsCapable b t r l m =>
                    HalfsSpecific b r l m
                 -> FilePath -> FileOffset
                 -> m Errno
halfsSetFileSize hsp@HS{ hspLogger = log } fp offset = do
  log $ "halfsSetFileSize: setting " ++ show fp ++ " to size " ++ show offset
  execDefault hsp $ setFileSize fp (fromIntegral offset)
         
halfsSetFileTimes :: HalfsCapable b t r l m =>
                     HalfsSpecific b r l m
                  -> FilePath -> EpochTime -> EpochTime
                  -> m Errno
halfsSetFileTimes hsp@HS{ hspLogger = log } fp accTm modTm = do
  -- TODO: Check perms: caller must be file owner w/ write access or
  -- superuser.
  accTm' <- fromCTime accTm
  modTm' <- fromCTime modTm
  log $ "halfsSetFileTimes fp = " ++ show fp ++ ", accTm = " ++ show accTm' ++
        ", modTm = " ++ show modTm'
  execDefault hsp $ setFileTimes fp accTm' modTm'     
         
halfsOpen :: HalfsCapable b t r l m =>
             HalfsSpecific b r l m             
          -> FilePath -> OpenMode -> OpenFileFlags
          -> m (Either Errno (FileHandle r l))
halfsOpen hsp@HS{ hspLogger = log } fp omode flags = do
  log $ "halfsOpen: fp = " ++ show fp ++ ", omode = " ++ show omode ++ 
        ", flags = " ++ show flags
  rslt <- execOrErrno hsp eINVAL id $ openFile fp halfsFlags
  log $ "halfsOpen: CoreAPI.openFile completed"
  return rslt
  where
    -- NB: In HFuse 0.2.2, the explicit and truncate are always false,
    -- so we do not pass them down to halfs.  Similarly, halfs does not
    -- support noctty, so it's ignored here as well.
    halfsFlags = H.FileOpenFlags
      { H.append   = append flags
      , H.nonBlock = nonBlock flags
      , H.openMode = case omode of
          ReadOnly  -> H.ReadOnly
          WriteOnly -> H.WriteOnly
          ReadWrite -> H.ReadWrite
      }

halfsRead :: HalfsCapable b t r l m =>
             HalfsSpecific b r l m
          -> FilePath -> FileHandle r l -> ByteCount -> FileOffset
          -> m (Either Errno BS.ByteString)
halfsRead hsp@HS{ hspLogger = log } fp fh byteCnt offset = do
  log $ "halfsRead: Reading " ++ show byteCnt ++ " bytes from " ++
        show fp ++ " at offset " ++ show offset
  execOrErrno hsp eINVAL id $ 
    read fh (fromIntegral offset) (fromIntegral byteCnt)

halfsWrite :: HalfsCapable b t r l m =>
              HalfsSpecific b r l m
           -> FilePath -> FileHandle r l -> BS.ByteString -> FileOffset
           -> m (Either Errno ByteCount)
halfsWrite hsp@HS{ hspLogger = log } fp fh bytes offset = do
  log $ "halfsWrite: Writing " ++ show (BS.length bytes) ++ " bytes to " ++ 
        show fp ++ " at offset " ++ show offset
  execOrErrno hsp eINVAL id $ do
    write fh (fromIntegral offset) bytes
    return (fromIntegral $ BS.length bytes)

halfsGetFileSystemStats :: HalfsCapable b t r l m =>
                           HalfsSpecific b r l m
                        -> FilePath
                        -> m (Either Errno System.Fuse.FileSystemStats)
halfsGetFileSystemStats hsp _fp = do
  execOrErrno hsp eINVAL fss2fss fsstat
  where
    fss2fss (FSS bs bc bf ba fc ff fa) = System.Fuse.FileSystemStats
      { fsStatBlockSize     = bs
      , fsStatBlockCount    = bc
      , fsStatBlocksFree    = bf
      , fsStatBlocksAvail   = ba
      , fsStatFileCount     = fc
      , fsStatFilesFree     = ff
      , fsStatFilesAvail    = fa
      , fsStatMaxNameLength = maxNameLength
      }

halfsFlush :: HalfsCapable b t r l m =>
              HalfsSpecific b r l m
           -> FilePath -> FileHandle r l
           -> m Errno
halfsFlush hsp@HS{ hspLogger = log } fp fh = do
  log $ "halfsFlush: Flushing " ++ show fp
  execDefault hsp $ flush fh
         
halfsRelease :: HalfsCapable b t r l m =>
                HalfsSpecific b r l m
             -> FilePath -> FileHandle r l
             -> m ()
halfsRelease HS{ hspLogger = log, hspState = fs } fp fh = do
  log $ "halfsRelease: Releasing " ++ show fp
  exec fs $ closeFile fh
         
halfsSyncFile :: HalfsCapable b t r l m =>
                 HalfsSpecific b r l m
              -> FilePath -> System.Fuse.SyncType
              -> m Errno
halfsSyncFile HS{ hspLogger = _log, hspState = _fs } _fp _syncType = do
  error "halfsSyncFile: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsOpenDirectory :: HalfsCapable b t r l m =>
                      HalfsSpecific b r l m
                   -> FilePath
                   -> m Errno
halfsOpenDirectory hsp@HS{ hspLogger = log, hspFpdhMap = fpdhMap } fp = do
  log $ "halfsOpenDirectory: fp = " ++ show fp
  execDefault hsp $ 
    withLockedRscRef fpdhMap $ \ref -> do
      mdh <- lookupRM fp ref
      case mdh of
        Nothing -> openDir fp >>= modifyRef ref . M.insert fp 
        _       -> return ()
  
halfsReadDirectory :: HalfsCapable b t r l m =>  
                      HalfsSpecific b r l m
                   -> FilePath
                   -> m (Either Errno [(FilePath, FileStat)])
halfsReadDirectory hsp@HS{ hspLogger = log, hspFpdhMap = fpdhMap } fp = do
  log $ "halfsReadDirectory: fp = " ++ show fp
  rslt <- execOrErrno hsp eINVAL id $ 
    withLockedRscRef fpdhMap $ \ref -> do
      mdh <- lookupRM fp ref
      case mdh of
        Nothing -> throwError HE_DirectoryHandleNotFound
        Just dh -> readDir dh
                     >>= mapM (\(p, s) -> (,) p `fmap` hfstat2fstat s)
  log $ "  rslt = " ++ show rslt
  return rslt

halfsReleaseDirectory :: HalfsCapable b t r l m =>
                         HalfsSpecific b r l m
                      -> FilePath
                      -> m Errno
halfsReleaseDirectory hsp@HS{ hspLogger = log, hspFpdhMap = fpdhMap} fp = do
  log $ "halfsReleaseDirectory: fp = " ++ show fp
  execDefault hsp $ 
    withLockedRscRef fpdhMap $ \ref -> do
      mdh <- lookupRM fp ref
      case mdh of
        Nothing -> throwError HE_DirectoryHandleNotFound
        Just dh -> closeDir dh >> modifyRef ref (M.delete fp)
         
halfsSyncDirectory :: HalfsCapable b t r l m =>
                      HalfsSpecific b r l m
                   -> FilePath -> System.Fuse.SyncType
                   -> m Errno
halfsSyncDirectory HS{ hspLogger = _log, hspState = _fs } _fp _syncType = do
  error "halfsSyncDirectory: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsAccess :: HalfsCapable b t r l m =>
               HalfsSpecific b r l m
            -> FilePath -> Int
            -> m Errno
halfsAccess (HS log _fs _fpdhMap) fp n = do
  log $ "halfsAccess: fp = " ++ show fp ++ ", n = " ++ show n
  return eOK -- TODO FIXME currently grants all access!
         
halfsInit :: HalfsCapable b t r l m =>
             HalfsSpecific b r l m
          -> m ()
halfsInit (HS log _fs _fpdhMap) = do
  log $ "halfsInit: Invoked."
  return ()

halfsDestroy :: HalfsCapable b t r l m =>
                HalfsSpecific b r l m
             -> m ()
halfsDestroy HS{ hspLogger = log, hspState = fs } = do
  log $ "halfsDestroy: Unmounting..." 
  exec fs $ unmount
  log "halfsDestroy: Shutting block device down..."        
  exec fs $ lift $ bdShutdown (hsBlockDev fs)
  log $ "halfsDestroy: Done."
  return ()

--------------------------------------------------------------------------------
-- Converters

hfstat2fstat :: (Show t, Timed t m) => H.FileStat t -> m FileStat 
hfstat2fstat stat = do
  atm  <- toCTime $ H.fsAccessTime stat
  mtm  <- toCTime $ H.fsModifyTime stat
  chtm <- toCTime $ H.fsChangeTime stat
  let entryType = case H.fsType stat of
                    H.RegularFile -> RegularFile
                    H.Directory   -> Directory
                    H.Symlink     -> SymbolicLink
                    -- TODO: Represent remaining FUSE entry types
                    H.AnyFileType -> error "Invalid fstat type"
  return FileStat
    { statEntryType        = entryType
    , statFileMode         = entryTypeToFileMode entryType
                             .|. emode (H.fsMode stat)
    , statLinkCount        = chkb16 $ H.fsNumLinks stat
    , statFileOwner        = chkb32 $ H.fsUID stat
    , statFileGroup        = chkb32 $ H.fsGID stat
    , statSpecialDeviceID  = 0 -- XXX/TODO: Do we need to distinguish by
                               -- blkdev, or is this for something else?
    , statFileSize         = fromIntegral $ H.fsSize stat
    , statBlocks           = fromIntegral $ H.fsNumBlocks stat
    , statAccessTime       = atm
    , statModificationTime = mtm
    , statStatusChangeTime = chtm
    }
  where
    chkb16 x = assert (x' <= fromIntegral (maxBound :: Word16)) x'
               where x' = fromIntegral x
    chkb32 x = assert (x' <= fromIntegral (maxBound :: Word32)) x'
               where x' = fromIntegral x
    emode (H.FileMode o g u) = cvt o * 0o100 + cvt g * 0o10 + cvt u where
      cvt = foldr (\p acc -> acc + case p of H.Read    -> 0o4
                                             H.Write   -> 0o2
                                             H.Execute -> 0o1
                  ) 0

-- NB: Something like this should probably be done by HFuse. TODO: Migrate?
mode2hmode :: FileMode -> H.FileMode
mode2hmode mode = H.FileMode (perms 6) (perms 3) (perms 0)
  where
    msk b   = case b of H.Read -> 4; H.Write -> 2; H.Execute -> 1
    perms k = chk H.Read ++ chk H.Write ++ chk H.Execute 
      where chk b = if mode .&. msk b `shiftL` k /= 0 then [b] else []

--------------------------------------------------------------------------------
-- Misc

exec :: HalfsCapable b t r l m =>
        HalfsState b r l m -> HalfsM b r l m a -> m a
exec fs act =
  runHalfs fs act >>= \ea -> case ea of
    Left e  -> fail $ show e
    Right x -> return x

execNoEnv :: Monad m => HalfsM b r l m a -> m a
execNoEnv act =
  runHalfsNoEnv act >>= \ea -> case ea of
    Left e  -> fail $ show e
    Right x -> return x

-- | Returns the result of the given action on success, otherwise yields an
-- Errno for the failed operation.  The Errno comes from either a HalfsM
-- exception signifying it holds an Errno, or the default provided as the first
-- argument.

execOrErrno :: Monad m =>
               HalfsSpecific b r l m
            -> Errno
            -> (a -> rslt)
            -> HalfsM b r l m a
            -> m (Either Errno rslt)
execOrErrno HS{ hspLogger = log, hspState = fs } defaultEn f act = do
 runHalfs fs act >>= \ea -> case ea of
   Left (HE_ErrnoAnnotated e en) -> do
     log ("execOrErrno: e = " ++ show e)
     return $ Left en
   Left e                        -> do
     log ("execOrErrno: e = " ++ show e)
     return $ Left defaultEn
   Right x                       -> return $ Right (f x)

execToErrno :: HalfsCapable b t r l m =>
               HalfsSpecific b r l m 
            -> Errno
            -> (a -> Errno)
            -> HalfsM b r l m a
            -> m Errno
execToErrno hsp defaultEn f = liftM (either id id) . execOrErrno hsp defaultEn f

execDefault :: HalfsCapable b t r l m =>
               HalfsSpecific b r l m -> HalfsM b r l m a -> m Errno
execDefault hsp = execToErrno hsp eINVAL (const eOK)

withLogger :: HalfsCapable b t r l m =>
              Logger m -> HalfsState b r l m -> HalfsState b r l m 
withLogger log fs = fs {hsLogger = Just log}

--------------------------------------------------------------------------------
-- Command line stuff

data Options = Options
  { optFileDev :: Maybe FilePath
  , optMemDev  :: Bool
  , optNumSecs :: Word64
  , optSecSize :: Word64
  , optLogFile :: FilePath
  }
  deriving (Show)

defOpts :: Options
defOpts = Options
  { optFileDev = Nothing
  , optMemDev  = True
  , optNumSecs = 512
  , optSecSize = 512
  , optLogFile = "halfs.log"
  } 

options :: [OptDescr (Options -> Options)]
options =
  [ Option ['m'] ["memdev"]
      (NoArg $ \opts -> opts{ optFileDev = Nothing, optMemDev = True })
      "use memory device"
  , Option ['f'] ["filedev"]
      (ReqArg (\f opts -> opts{ optFileDev = Just f, optMemDev = False })
              "PATH"
      )
      "use file-backed device"
  , Option ['n'] ["numsecs"]
      (ReqArg (\s0 opts -> let s1 = Prelude.read s0 in opts{ optNumSecs = s1 })
              "SIZE"
      )
      "number of sectors (ignored for filedevs; default 512)"
  , Option ['s'] ["secsize"]
      (ReqArg (\s0 opts -> let s1 = Prelude.read s0 in opts{ optSecSize = s1 })
              "SIZE"
      )
      "sector size in bytes (ignored for existing filedevs; default 512)"
  , Option ['l'] ["logfile"]
      (ReqArg (\s opts -> opts{ optLogFile = s })
              "PATH"
      )
      "filename for halfs logging (default ./halfs.log)"
  ]

--------------------------------------------------------------------------------
-- Instances for debugging

instance Show OpenMode where
  show ReadOnly  = "ReadOnly"
  show WriteOnly = "WriteOnly"
  show ReadWrite = "ReadWrite"

instance Show OpenFileFlags where
  show (OpenFileFlags append' exclusive' noctty' nonBlock' trunc') =
    "OpenFileFlags " ++
    "{ append = "    ++ show append'    ++
    ", exclusive = " ++ show exclusive' ++
    ", noctty = "    ++ show noctty'    ++
    ", nonBlock = "  ++ show nonBlock'  ++
    ", trunc = "     ++ show trunc'     ++
    "}"

-- Get rid of extraneous Halfs.File not-used warning
_dummy :: HalfsCapable b t r l m =>
          H.InodeRef -> HalfsM b r l m (FileHandle r l)
_dummy = openFilePrim undefined