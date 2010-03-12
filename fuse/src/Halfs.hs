{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Command line tool for mounting a halfs filesystem from userspace via
-- hFUSE/FUSE.

module Main
where

import Control.Applicative
import Control.Exception     (assert)
import Data.Array.IO         (IOUArray)
import Data.IORef            (IORef)
import Data.Word             
import System.Console.GetOpt 
import System.Directory      (doesFileExist)
import System.Environment
import System.IO
import System.Posix.Types    ( ByteCount
                             , DeviceID
                             , EpochTime
                             , FileMode
                             , FileOffset
                             , GroupID
                             , UserID
                             )

import System.Fuse     

import Halfs.Classes
import Halfs.CoreAPI
import Halfs.File (FileHandle)
import Halfs.HalfsState
import Halfs.Monad
import System.Device.BlockDevice
import System.Device.File
import System.Device.Memory
import Tests.Utils

import qualified Data.ByteString as BS
import qualified Halfs.Types     as H
import Prelude hiding (log, catch)  

-- Halfs-specific stuff we carry around in our FUSE functions; note that the
-- FUSE library does this via opaque ptr to user data, but since hFUSE
-- reimplements fuse_main and doesn't seem to provide a way to hang onto private
-- data, we just carry the data ourselves.
type Logger m              = String -> m ()
type HalfsSpecific b r l m = (Logger m, HalfsState b r l m)

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
                 else withFileStore False fp sz n (`newFileBlockDevice` sz)
                        <* putStrLn "Created filedev from new file."  

  when (not exists) $ exec $ newfs dev >> return ()
  fs <- exec $ mount dev

  let log s = hPutStrLn stderr s
  withArgs argv1 $ fuseMain (ops (log, fs)) $ \e -> do
    log $ "*** Exception: " ++ show e
    return eFAULT

--------------------------------------------------------------------------------
-- Halfs-hFUSE filesystem operation implementation

-- JS: ST monad impls will have to get mapped to hFUSE ops via stToIO?

ops :: HalfsSpecific (IOUArray Word64 Bool) IORef IOLock IO
    -> FuseOperations FileHandle
ops hsp@(log,fs) = defaultFuseOps
  { fuseGetFileStat        = halfsGetFileStat        hsp
  , fuseGetFileSystemStats = halfsGetFileSystemStats hsp
  , fuseInit               = halfsInit               hsp
  , fuseDestroy            = halfsDestroy            hsp
  }

{-
ops hsp@(_log, _) = FuseOperations
  { fuseGetFileStat          = halfsGetFileStat          hsp
  , fuseReadSymbolicLink     = halfsReadSymbolicLink     hsp
  , fuseCreateDevice         = halfsCreateDevice         hsp
  , fuseCreateDirectory      = halfsCreateDirectory      hsp
  , fuseRemoveLink           = halfsRemoveLink           hsp
  , fuseRemoveDirectory      = halfsRemoveDirectory      hsp
  , fuseCreateSymbolicLink   = halfsCreateSymbolicLink   hsp
  , fuseRename               = halfsRename               hsp
  , fuseCreateLink           = halfsCreateLink           hsp
  , fuseSetFileMode          = halfsSetFileMode          hsp
  , fuseSetOwnerAndGroup     = halfsSetOwnerAndGroup     hsp
  , fuseSetFileSize          = halfsSetFileSize          hsp
  , fuseSetFileTimes         = halfsSetFileTimes         hsp
  , fuseOpen                 = halfsOpen                 hsp
  , fuseRead                 = halfsRead                 hsp
  , fuseWrite                = halfsWrite                hsp
  , fuseGetFileSystemStats   = halfsGetFileSystemStats   hsp
  , fuseFlush                = halfsFlush                hsp
  , fuseRelease              = halfsRelease              hsp
  , fuseSynchronizeFile      = halfsSynchronizeFile      hsp
  , fuseOpenDirectory        = halfsOpenDirectory        hsp
  , fuseReadDirectory        = halfsReadDirectory        hsp
  , fuseReleaseDirectory     = halfsReleaseDirectory     hsp
  , fuseSynchronizeDirectory = halfsSynchronizeDirectory hsp
  , fuseAccess               = halfsAccess               hsp
  , fuseInit                 = halfsInit                 hsp
  , fuseDestroy              = halfsDestroy              hsp
  }
-}

halfsGetFileStat :: HalfsCapable b t r l m =>
                    HalfsSpecific b r l m
                 -> FilePath
                 -> m (Either Errno FileStat)
halfsGetFileStat (log, fs) fp = do
  error "halfsGetFileStat: Not Yet Implemented." -- TODO
{-
  log $ replicate 80 '-'
  log $ "halfsGetFileStat entry: fp = " ++ show fp
  eestat <- execOrErrno eINVAL id (fstat fs fp)
  case eestat of
    Left _     -> do
      log $ "halfsGetFileStat: fstat failed."
      return $ f2f undefined undefined `fmap` eestat
    Right stat -> do
      log $ "halfsGetFileStat: Halfs.Types.FileStat = " ++ show stat
      atm <- toCTime $ H.fsAccessTime stat
      mtm <- toCTime $ H.fsModTime stat
      return $ Right $ f2f atm mtm stat
  where
    chkb16 x = assert (x' <= fromIntegral (maxBound :: Word16)) x'
               where x' = fromIntegral x
    chkb32 x = assert (x' <= fromIntegral (maxBound :: Word32)) x'
               where x' = fromIntegral x
    --
    f2f atm mtm stat =
      let entryType = case H.fsType stat of
                        H.RegularFile -> RegularFile
                        H.Directory   -> Directory
                        H.Symlink     -> SymbolicLink
                        -- TODO: Represent remaining FUSE entry types
                        H.AnyFileType -> error "Invalid fstat type"
      in FileStat
        { statEntryType        = entryType
        , statFileMode         = entryTypeToFileMode entryType
        , statLinkCount        = chkb16 $ H.fsNumLinks stat
        , statFileOwner        = chkb32 $ H.fsUID stat
        , statFileGroup        = chkb32 $ H.fsGID stat
        , statSpecialDeviceID  = 0 -- XXX/TODO: Do we need to distinguish by
                                   -- blkdev, or is this for something else?
        , statFileSize         = fromIntegral $ H.fsSize stat
        , statBlocks           = fromIntegral $ H.fsNumBlocks stat
        , statAccessTime       = atm
        , statModificationTime = mtm
        , statStatusChangeTime = 0 -- FIXME: We need to track this
                                   -- error "Status change time not yet computed"
        }
-}

halfsReadSymbolicLink :: HalfsCapable b t r l m =>
                         HalfsSpecific b r l m
                      -> FilePath
                      -> m (Either Errno FilePath)
halfsReadSymbolicLink (log, _fs) _fp = do 
  error "halfsReadSymbolicLink: Not Yet Implemented" -- TODO
  return (Left eNOSYS)

halfsCreateDevice :: HalfsCapable b t r l m =>
                     HalfsSpecific b r l m
                  -> FilePath -> EntryType -> FileMode -> DeviceID
                  -> m Errno
halfsCreateDevice (log, _fs) _fp _etype _mode _devID = do
  error "halfsCreateDevice: Not Yet Implemented." -- TODO
  return eNOSYS

halfsCreateDirectory :: HalfsCapable b t r l m =>
                        HalfsSpecific b r l m
                     -> FilePath -> FileMode
                     -> m Errno
halfsCreateDirectory (log, _fs) _fp _mode = do
  error "halfsCreateDirectory: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsRemoveLink :: HalfsCapable b t r l m =>
                   HalfsSpecific b r l m
                -> FilePath
                -> m Errno
halfsRemoveLink (log, _fs) _fp = do
  error "halfsRemoveLink: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsRemoveDirectory :: HalfsCapable b t r l m =>
                        HalfsSpecific b r l m
                     -> FilePath
                     -> m Errno
halfsRemoveDirectory (log, _fs) _fp = do
  error "halfsRemoveDirectory: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsCreateSymbolicLink :: HalfsCapable b t r l m =>
                           HalfsSpecific b r l m
                        -> FilePath -> FilePath
                        -> m Errno
halfsCreateSymbolicLink (log, _fs) _src _dst = do
  error $ "halfsCreateSymbolicLink: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsRename :: HalfsCapable b t r l m =>
               HalfsSpecific b r l m
            -> FilePath -> FilePath
            -> m Errno
halfsRename (log, _fs) _old _new = do
  error $ "halfsRename: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsCreateLink :: HalfsCapable b t r l m =>
                   HalfsSpecific b r l m
                -> FilePath -> FilePath
                -> m Errno
halfsCreateLink (log, _fs) _src _dst = do
  error $ "halfsCreateLink: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsSetFileMode :: HalfsCapable b t r l m =>
                    HalfsSpecific b r l m
                 -> FilePath -> FileMode
                 -> m Errno
halfsSetFileMode (log, _fs) _fp _mode = do
  error $ "halfsSetFileMode: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsSetOwnerAndGroup :: HalfsCapable b t r l m =>
                         HalfsSpecific b r l m
                      -> FilePath -> UserID -> GroupID
                      -> m Errno
halfsSetOwnerAndGroup (log, _fs) _fp _uid _gid = do
  error $ "halfsSetOwnerAndGroup: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsSetFileSize :: HalfsCapable b t r l m =>
                    HalfsSpecific b r l m
                 -> FilePath -> FileOffset
                 -> m Errno
halfsSetFileSize (log, _fs) _fp _offset = do
  error $ "halfsSetFileSize: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsSetFileTimes :: HalfsCapable b t r l m =>
                     HalfsSpecific b r l m
                  -> FilePath -> EpochTime -> EpochTime
                  -> m Errno
halfsSetFileTimes (log, _fs) _fp _tm0 _tm1 = do
  error $ "halfsSetFileTimes: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsOpen :: HalfsCapable b t r l m =>
             HalfsSpecific b r l m             
          -> FilePath -> OpenMode -> OpenFileFlags
          -> m (Either Errno FileHandle)
halfsOpen (log, _fs) fp mode flags = do
  error $ "halfsOpen: Not Yet Implemented." -- TODO
  return (Left eNOSYS)

halfsRead :: HalfsCapable b t r l m =>
             HalfsSpecific b r l m
          -> FilePath -> FileHandle -> ByteCount -> FileOffset
          -> m (Either Errno BS.ByteString)
halfsRead (log, _fs) fp _fh byteCnt offset = do
  error $ "halfsRead: Not Yet Implemented." -- TODO
  return (Left eNOSYS)

halfsWrite :: HalfsCapable b t r l m =>
              HalfsSpecific b r l m
           -> FilePath -> FileHandle -> BS.ByteString -> FileOffset
           -> m (Either Errno ByteCount)
halfsWrite (log, _fs) _fp _fh _bytes _offset = do
  error $ "halfsWrite: Not Yet Implemented." -- TODO
  return (Left eNOSYS)

halfsGetFileSystemStats :: HalfsCapable b t r l m =>
                           HalfsSpecific b r l m
                        -> FilePath
                        -> m (Either Errno System.Fuse.FileSystemStats)
halfsGetFileSystemStats (log, fs) fp = do
--  error "halfsGetFileSystemStats: Not Yet Implemented." -- TODO
--  return $ Left $ eNOTTY
  log $ "halfsGetFileSystemStats: fp = " ++ show fp
  return $ Right $ System.Fuse.FileSystemStats 512 16 8 8 1 3 255
{-
  log $ "halfsGetFileSystemStats: fp = " ++ show fp
  -- TODO: execOrErrno eINVAL fss2fss (fsstat fs)
  x <- execOrErrno eINVAL id (fsstat fs)
  log $ "Halfs.Types.FileSystemStats: " ++ show x
  return (fss2fss `fmap` x)
  where
    fss2fss (FSS bs bc bf ba fc ff) = System.Fuse.FileSystemStats
      { fsStatBlockSize       = bs
      , fsStatBlockCount      = bc
      , fsStatBlocksFree      = bf
      , fsStatBlocksAvailable = ba
      , fsStatFileCount       = fc
      , fsStatFilesFree       = ff
      , fsStatMaxNameLength   = maxNameLength
      }
-}

halfsFlush :: HalfsCapable b t r l m =>
              HalfsSpecific b r l m
           -> FilePath -> FileHandle
           -> m Errno
halfsFlush (log, _fs) _fp _fh = do
  error "halfsFlush: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsRelease :: HalfsCapable b t r l m =>
                HalfsSpecific b r l m
             -> FilePath -> FileHandle
             -> m ()
halfsRelease (log, _fs) _fp _fh = do
  error "halfsRelease: Not Yet Implemented." -- TODO
  return ()
         
halfsSynchronizeFile :: HalfsCapable b t r l m =>
                        HalfsSpecific b r l m
                     -> FilePath -> System.Fuse.SyncType
                     -> m Errno
halfsSynchronizeFile (log, _fs) _fp _syncType = do
  error "halfsSynchronizeFile: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsOpenDirectory :: HalfsCapable b t r l m =>
                      HalfsSpecific b r l m
                   -> FilePath
                   -> m Errno
halfsOpenDirectory (log, _fs) fp = do
  error "halfsOpenDirectory: Not Yet Implemented." -- TODO
  return eNOSYS

halfsReadDirectory :: HalfsCapable b t r l m =>  
                      HalfsSpecific b r l m
                   -> FilePath
                   -> m (Either Errno [(FilePath, FileStat)])
halfsReadDirectory (log, _fs) fp = do
  error "halfsReadDirectory: Not Yet Implemented." -- TODO
  return (Left eNOSYS)

halfsReleaseDirectory :: HalfsCapable b t r l m =>
                         HalfsSpecific b r l m
                      -> FilePath
                      -> m Errno
halfsReleaseDirectory (log, _fs) _fp = do
  error "halfsReleaseDirectory: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsSynchronizeDirectory :: HalfsCapable b t r l m =>
                             HalfsSpecific b r l m
                          -> FilePath -> System.Fuse.SyncType
                          -> m Errno
halfsSynchronizeDirectory (log, _fs) _fp _syncType = do
  error "halfsSynchronizeDirectory: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsAccess :: HalfsCapable b t r l m =>
               HalfsSpecific b r l m
            -> FilePath -> Int
            -> m Errno
halfsAccess (log, _fs) _fp _n = do
  error "halfsAccess: Not Yet Implemented." -- TODO
  return eNOSYS
         
halfsInit :: HalfsCapable b t r l m =>
             HalfsSpecific b r l m
          -> m ()
halfsInit (log, _fs) = do
--  error "halfsInit: Not Yet Implemented." -- TODO
  log $ "halfsInit: Invoked."
  return ()

halfsDestroy :: HalfsCapable b t r l m =>
                HalfsSpecific b r l m
             -> m ()
halfsDestroy (log, fs) = do
  log $ "halfsDestroy: Unmounting..." 
  exec $ unmount fs
  log "halfsDestroy: Shutting block device down..."        
  exec $ lift $ bdShutdown (hsBlockDev fs)
  log $ "halfsDestroy: Done."
  return ()

--------------------------------------------------------------------------------
-- Misc

exec :: Monad m => HalfsT m a -> m a
exec act =
  runHalfs act >>= \ea -> case ea of
    Left e  -> fail $ show e
    Right x -> return x

execOrErrno :: Monad m => Errno -> (a -> b) -> HalfsT m a -> m (Either Errno b)
execOrErrno en f act = do
 runHalfs act >>= \ea -> case ea of
   Left _e -> return $ Left en
   Right x -> return $ Right (f x)

--------------------------------------------------------------------------------
-- Command line stuff

data Options = Options
  { optFileDev :: Maybe FilePath
  , optMemDev  :: Bool
  , optNumSecs :: Word64
  , optSecSize :: Word64
  }
  deriving (Show)

defOpts :: Options
defOpts = Options
  { optFileDev = Nothing
  , optMemDev  = True
  , optNumSecs = 512
  , optSecSize = 512
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
      "number of sectors (ignored for filedevs)"
  , Option ['s'] ["secsize"]
      (ReqArg (\s0 opts -> let s1 = Prelude.read s0 in opts{ optSecSize = s1 })
              "SIZE"
      )
      "sector size in bytes (ignored for already-existing filedevs)"
  ]

--------------------------------------------------------------------------------
-- Instances for debugging

instance Show OpenMode where
  show ReadOnly  = "ReadOnly"
  show WriteOnly = "WriteOnly"
  show ReadWrite = "ReadWrite"

instance Show OpenFileFlags where
  show (OpenFileFlags append' exclusive' noctty' nonBlock' trunc') =
    "OpenFileFlags { append    = " ++ show append'    ++    
    "                exclusive = " ++ show exclusive' ++ 
    "                noctty    = " ++ show noctty'    ++    
    "                nonBlock  = " ++ show nonBlock'  ++  
    "                trunc     = " ++ show trunc'
