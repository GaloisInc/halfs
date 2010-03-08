{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main
where

import Control.Applicative
import qualified Data.ByteString as BS
import Data.Word
import Prelude hiding (log)
import System.Directory   (doesFileExist, getCurrentDirectory)
import System.IO
import System.Posix.Types (ByteCount, FileOffset)

import System.Console.GetOpt
import System.Environment

import System.Fuse

import Halfs.File (FileHandle)
import System.Device.File
import System.Device.Memory
import Tests.Utils

type Logger = String -> IO ()

log' :: Handle -> Logger
log' h s = hPutStrLn h s >> hFlush h

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
      (ReqArg (\s0 opts -> let s1 = read s0 in opts{ optNumSecs = s1 })
              "SIZE"
      )
      "number of sectors (ignored for filedevs)"
  , Option ['s'] ["secsize"]
      (ReqArg (\s0 opts -> let s1 = read s0 in opts{ optSecSize = s1 })
              "SIZE"
      )
      "sector size in bytes (ignored for already-existing filedevs)"
  ]

main :: IO ()
main = do
  (opts, argv1) <- do
    argv0 <- getArgs
    case getOpt RequireOrder options argv0 of
      (o, n, [])   -> return (foldl (flip ($)) defOpts o, n)
      (_, _, errs) -> ioError $ userError $ concat errs ++ usageInfo hdr options
        where hdr = "Usage: halfs [OPTION...] <FUSE CMDLINE>"

  let sz = optSecSize opts; n = optNumSecs opts
  dev <- maybe (fail "Unable to create device") return
    =<<
    if optMemDev opts
     then newMemoryBlockDevice n sz <* putStrLn "Created memory device."
     else case optFileDev opts of
            Nothing -> fail "Can't happen"
            Just fp -> do
              exists <- doesFileExist fp
              if exists
               then newFileBlockDevice fp sz 
                      <* putStrLn "Created file device from existing file."
               else withFileStore False fp sz n (`newFileBlockDevice` sz)
                      <* putStrLn "Created file device from new file."  

  -- Successful dev acquisition and newfs/mountfs invocation from the above will
  -- yield a HalfsState structure that can be passed to hOps and mixed in with
  -- the various filesystem operations.  Note that the true fuse library way to
  -- do this is via opaque ptr to user data, but since HFuse reimplements
  -- fuse_main, we carry our filesystem-private data ourselves.

  (_, logH) <- flip openTempFile "halfs.log" =<< getCurrentDirectory
  let log = log' logH
  
  withArgs argv1 $ fuseMain (hOps log) defaultExceptionHandler

hOps :: Logger -> FuseOperations FileHandle
hOps log = defaultFuseOps
             { fuseGetFileStat        = halfsGetFileStat        log
             , fuseOpen               = halfsOpen               log
             , fuseRead               = halfsRead               log
             , fuseOpenDirectory      = halfsOpenDirectory      log
             , fuseReadDirectory      = halfsReadDirectory      log
             , fuseGetFileSystemStats = halfsGetFileSystemStats log
             }

halfsGetFileStat :: Logger -> FilePath
                 -> IO (Either Errno FileStat)
halfsGetFileStat log fp = do
  log $ "halfsGetFileStat: fp = " ++ show fp
  return (Left eOK)

halfsOpen :: Logger -> FilePath -> OpenMode -> OpenFileFlags
          -> IO (Either Errno FileHandle)
halfsOpen log fp mode flags = do
  log $ "halfsOpen: fp = " ++ show fp ++ ", mode = " ++ show mode ++ ", flags = " ++ show flags
  return (Left eOK)

halfsRead :: Logger -> FilePath -> FileHandle -> ByteCount -> FileOffset
          -> IO (Either Errno BS.ByteString)
halfsRead log fp _fh byteCnt offset = do
  log $ "halfsRead: fp = " ++ show fp ++ ", byteCnt = " ++ show byteCnt ++ ", offset = " ++ show offset
  return (Left eOK)

halfsOpenDirectory :: Logger -> FilePath
                   -> IO Errno
halfsOpenDirectory log fp = do
  log $ "halfsOpenDirectory: fp = " ++ show fp
  return eOK

halfsReadDirectory :: Logger -> FilePath
                   -> IO (Either Errno [(FilePath, FileStat)])
halfsReadDirectory log fp = do
  log $ "halfsReadDirectory: fp = " ++ show fp
  return (Left eOK)

halfsGetFileSystemStats :: Logger -> FilePath ->
                           IO (Either Errno FileSystemStats)
halfsGetFileSystemStats log fp = do
  log $ "halfsGetFileSystemStats: fp = " ++ show fp
  return (Left eOK)

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
