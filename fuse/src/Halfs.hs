{-# LANGUAGE Rank2Types #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main
where

import Control.Applicative
import Data.Array.IO (IOUArray)
import Data.IORef (IORef)
import Data.Word
import Prelude hiding (log)
import System.Console.GetOpt
import System.Directory   (doesFileExist, getCurrentDirectory)
import System.Environment
import System.IO
import System.Posix.Types ( ByteCount
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
import System.Device.File
import System.Device.Memory
import Tests.Utils

import qualified Data.ByteString as BS

-- Halfs-specific stuff we carry around in our FUSE functions
type Logger m              = String -> m ()
type HalfsSpecific b r l m = (Logger m, HalfsState b r l m)

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

  -- Successful dev acquisition and newfs/mountfs invocation from the above will
  -- yield a HalfsState structure that can be passed to hOps and mixed in with
  -- the various filesystem operations.  Note that the true fuse library way to
  -- do this is via opaque ptr to user data, but since HFuse reimplements
  -- fuse_main, we carry our filesystem-private data ourselves.

  -- TODO: exception handling and cleanup (Device flush and shutdown etc.)

  log <- liftM (logger . snd) $
           (`openTempFile` "halfs.log") =<< getCurrentDirectory

  withArgs argv1 $ fuseMain (ops (log, fs)) defaultExceptionHandler

--------------------------------------------------------------------------------
-- Halfs-FUSE filesystem operation implementation

ops :: HalfsSpecific (IOUArray Word64 Bool) IORef IOLock IO
    -> FuseOperations FileHandle
ops hspec = FuseOperations
  { fuseGetFileStat          = halfsGetFileStat          hspec
  , fuseReadSymbolicLink     = halfsReadSymbolicLink     hspec
  , fuseCreateDevice         = halfsCreateDevice         hspec
  , fuseCreateDirectory      = halfsCreateDirectory      hspec
  , fuseRemoveLink           = halfsRemoveLink           hspec
  , fuseRemoveDirectory      = halfsRemoveDirectory      hspec
  , fuseCreateSymbolicLink   = halfsCreateSymbolicLink   hspec
  , fuseRename               = halfsRename               hspec
  , fuseCreateLink           = halfsCreateLink           hspec
  , fuseSetFileMode          = halfsSetFileMode          hspec
  , fuseSetOwnerAndGroup     = halfsSetOwnerAndGroup     hspec
  , fuseSetFileSize          = halfsSetFileSize          hspec
  , fuseSetFileTimes         = halfsSetFileTimes         hspec
  , fuseOpen                 = halfsOpen                 hspec
  , fuseRead                 = halfsRead                 hspec
  , fuseWrite                = halfsWrite                hspec
  , fuseGetFileSystemStats   = halfsGetFileSystemStats   hspec
  , fuseFlush                = halfsFlush                hspec
  , fuseRelease              = halfsRelease              hspec
  , fuseSynchronizeFile      = halfsSynchronizeFile      hspec
  , fuseOpenDirectory        = halfsOpenDirectory        hspec
  , fuseReadDirectory        = halfsReadDirectory        hspec
  , fuseReleaseDirectory     = halfsReleaseDirectory     hspec
  , fuseSynchronizeDirectory = halfsSynchronizeDirectory hspec
  , fuseAccess               = halfsAccess               hspec
  , fuseInit                 = halfsInit                 hspec
  , fuseDestroy              = halfsDestroy              hspec
  }

halfsGetFileStat :: HalfsCapable b t r l m =>
                    HalfsSpecific b r l m
                 -> FilePath
                 -> m (Either Errno FileStat)
halfsGetFileStat (log, _fs) fp = do
  log $ "halfsGetFileStat: fp = " ++ show fp
  return (Left eOK)

halfsReadSymbolicLink :: HalfsCapable b t r l m =>
                         HalfsSpecific b r l m
                      -> FilePath
                      -> m (Either Errno FilePath)
halfsReadSymbolicLink (log, _fs) _fp = do 
  log $ "halfsReadSymbolicLink: Not Yet Implemented"
  return (Left eINVAL)

halfsCreateDevice :: HalfsCapable b t r l m =>
                     HalfsSpecific b r l m
                  -> FilePath -> EntryType -> FileMode -> DeviceID
                  -> m Errno
halfsCreateDevice (log, _fs) _fp _etype _mode _devID = do
  log $ "halfsCreateDevice: Not Yet Implemented."
  return eINVAL

halfsCreateDirectory :: HalfsCapable b t r l m =>
                        HalfsSpecific b r l m
                     -> FilePath -> FileMode
                     -> m Errno
halfsCreateDirectory (log, _fs) _fp _mode = do
  log $ "halfsCreateDirectory: Not Yet Implemented."
  return eINVAL
         
halfsRemoveLink :: HalfsCapable b t r l m =>
                   HalfsSpecific b r l m
                -> FilePath
                -> m Errno
halfsRemoveLink (log, _fs) _fp = do
  log $ "halfsRemoveLink: Not Yet Implemented."
  return eINVAL
         
halfsRemoveDirectory :: HalfsCapable b t r l m =>
                        HalfsSpecific b r l m
                     -> FilePath
                     -> m Errno
halfsRemoveDirectory (log, _fs) _fp = do
  log $ "halfsRemoveDirectory: Not Yet Implemented."
  return eINVAL
         
halfsCreateSymbolicLink :: HalfsCapable b t r l m =>
                           HalfsSpecific b r l m
                        -> FilePath -> FilePath
                        -> m Errno
halfsCreateSymbolicLink (log, _fs) _src _dst = do
  log $ "halfsCreateSymbolicLink: Not Yet Implemented."
  return eINVAL
         
halfsRename :: HalfsCapable b t r l m =>
               HalfsSpecific b r l m
            -> FilePath -> FilePath
            -> m Errno
halfsRename (log, _fs) _old _new = do
  log $ "halfsRename: Not Yet Implemented."
  return eINVAL
         
halfsCreateLink :: HalfsCapable b t r l m =>
                   HalfsSpecific b r l m
                -> FilePath -> FilePath
                -> m Errno
halfsCreateLink (log, _fs) _src _dst = do
  log $ "halfsCreateLink: Not Yet Implemented."
  return eINVAL
         
halfsSetFileMode :: HalfsCapable b t r l m =>
                    HalfsSpecific b r l m
                 -> FilePath -> FileMode
                 -> m Errno
halfsSetFileMode (log, _fs) _fp _mode = do
  log $ "halfsSetFileMode: Not Yet Implemented."
  return eINVAL
         
halfsSetOwnerAndGroup :: HalfsCapable b t r l m =>
                         HalfsSpecific b r l m
                      -> FilePath -> UserID -> GroupID
                      -> m Errno
halfsSetOwnerAndGroup (log, _fs) _fp _uid _gid = do
  log $ "halfsSetOwnerAndGroup: Not Yet Implemented."
  return eINVAL
         
halfsSetFileSize :: HalfsCapable b t r l m =>
                    HalfsSpecific b r l m
                 -> FilePath -> FileOffset
                 -> m Errno
halfsSetFileSize (log, _fs) _fp _offset = do
  log $ "halfsSetFileSize: Not Yet Implemented."
  return eINVAL
         
halfsSetFileTimes :: HalfsCapable b t r l m =>
                     HalfsSpecific b r l m
                  -> FilePath -> EpochTime -> EpochTime
                  -> m Errno
halfsSetFileTimes (log, _fs) _fp _tm0 _tm1 = do
  log $ "halfsSetFileTimes: Not Yet Implemented."
  return eINVAL
         
halfsOpen :: HalfsCapable b t r l m =>
             HalfsSpecific b r l m             
          -> FilePath -> OpenMode -> OpenFileFlags
          -> m (Either Errno FileHandle)
halfsOpen (log, _fs) fp mode flags = do
  log $ "halfsOpen: fp = " ++ show fp ++ ", mode = " ++ show mode ++ ", flags = " ++ show flags
  return (Left eOK)

halfsRead :: HalfsCapable b t r l m =>
             HalfsSpecific b r l m
          -> FilePath -> FileHandle -> ByteCount -> FileOffset
          -> m (Either Errno BS.ByteString)
halfsRead (log, _fs) fp _fh byteCnt offset = do
  log $ "halfsRead: fp = " ++ show fp ++ ", byteCnt = " ++ show byteCnt ++ ", offset = " ++ show offset
  return (Left eOK)

halfsWrite :: HalfsCapable b t r l m =>
              HalfsSpecific b r l m
           -> FilePath -> FileHandle -> BS.ByteString -> FileOffset
           -> m (Either Errno ByteCount)
halfsWrite (log, _fs) _fp _fh _bytes _offset = do
  log $ "halfsWrite: Not Yet Implemented."
  return (Left eINVAL)

halfsGetFileSystemStats :: HalfsCapable b t r l m =>
                           HalfsSpecific b r l m
                        -> FilePath
                        -> m (Either Errno System.Fuse.FileSystemStats)
halfsGetFileSystemStats (log, _fs) fp = do
  log $ "halfsGetFileSystemStats: Not Yet Implemented. fp = " ++ show fp
  return (Left eOK)

halfsFlush :: HalfsCapable b t r l m =>
              HalfsSpecific b r l m
           -> FilePath -> FileHandle
           -> m Errno
halfsFlush (log, _fs) _fp _fh = do
  log $ "halfsFlush: Not Yet Implemented."
  return eINVAL
         
halfsRelease :: HalfsCapable b t r l m =>
                HalfsSpecific b r l m
             -> FilePath -> FileHandle
             -> m ()
halfsRelease (log, _fs) _fp _fh = do
  log $ "halfsRelease: Not Yet Implemented."
  return ()
         
halfsSynchronizeFile :: HalfsCapable b t r l m =>
                        HalfsSpecific b r l m
                     -> FilePath -> System.Fuse.SyncType
                     -> m Errno
halfsSynchronizeFile (log, _fs) _fp _syncType = do
  log $ "halfsSynchronizeFile: Not Yet Implemented."
  return eINVAL
         
halfsOpenDirectory :: HalfsCapable b t r l m =>
                      HalfsSpecific b r l m
                   -> FilePath
                   -> m Errno
halfsOpenDirectory (log, _fs) fp = do
  log $ "halfsOpenDirectory: fp = " ++ show fp
  return eOK

halfsReadDirectory :: HalfsCapable b t r l m =>  
                      HalfsSpecific b r l m
                   -> FilePath
                   -> m (Either Errno [(FilePath, FileStat)])
halfsReadDirectory (log, _fs) fp = do
  log $ "halfsReadDirectory: fp = " ++ show fp
  return (Left eOK)

halfsReleaseDirectory :: HalfsCapable b t r l m =>
                         HalfsSpecific b r l m
                      -> FilePath
                      -> m Errno
halfsReleaseDirectory (log, _fs) _fp = do
  log $ "halfsReleaseDirectory: Not Yet Implemented."
  return eINVAL
         
halfsSynchronizeDirectory :: HalfsCapable b t r l m =>
                             HalfsSpecific b r l m
                          -> FilePath -> System.Fuse.SyncType
                          -> m Errno
halfsSynchronizeDirectory (log, _fs) _fp _syncType = do
  log $ "halfsSynchronizeDirectory: Not Yet Implemented."
  return eINVAL
         
halfsAccess :: HalfsCapable b t r l m =>
               HalfsSpecific b r l m
            -> FilePath -> Int
            -> m Errno
halfsAccess (log, _fs) _fp _n = do
  log $ "halfsAccess: Not Yet Implemented."
  return eINVAL
         
halfsInit :: HalfsCapable b t r l m =>
             HalfsSpecific b r l m
          -> m ()
halfsInit (log, _fs) = do
  log $ "halfsInit: Not Yet Implemented."
  return ()

halfsDestroy :: HalfsCapable b t r l m =>
                HalfsSpecific b r l m
             -> m ()
halfsDestroy (log, fs) = do
  log "halfsDestroy: Unmounting..." 
  exec $ unmount fs
  log $ "halfsDestroy: Done."
  return ()

--------------------------------------------------------------------------------
-- Misc

logger :: Handle -> Logger IO
logger h s = do
  hPutStrLn h s 
  hFlush h
-- logger _ _ = return ()

exec :: Monad m => HalfsT m a -> m a
exec act = runHalfs act >>= \ea -> case ea of
  Left e  -> fail $ show e
  Right x -> return x

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
