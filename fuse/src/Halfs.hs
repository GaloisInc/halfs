{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main
where

import Data.ByteString hiding (hPutStrLn)
import Prelude hiding (log)
import System.Directory   (getCurrentDirectory)
import System.IO
import System.Posix.Types (ByteCount, FileOffset)

import System.Fuse

import Halfs.File (FileHandle)

type Logger = String -> IO ()

main :: IO ()
main = do
  -- TODO/HERE: Parse command line options which enable:
  --   * Creation of an empty FS using a new memdev
  --   * Creation of an empty FS using a new file-backed dev
  --   * Using an existing file-backed dev
  --
  -- Chop these processed options off of args via withArgs before handing down
  -- to fuseMain.
  --
  -- Successful dev acquisition and newfs/mountfs invocation from the above will
  -- yield a HalfsState structure that can be passed to hOps and mixed in with
  -- the various filesystem operations.  Note that the true fuse library way to
  -- do this is via opaque ptr to user data, but since HFuse reimplements
  -- fuse_main, we carry our filesystem-private data ourselves.
  -- 

  (_, logH) <- flip openTempFile "halfs.log" =<< getCurrentDirectory
  fuseMain (hOps logH) defaultExceptionHandler

hOps :: Handle -> FuseOperations FileHandle
hOps logH = defaultFuseOps
             { fuseGetFileStat        = halfsGetFileStat        log
             , fuseOpen               = halfsOpen               log
             , fuseRead               = halfsRead               log
             , fuseOpenDirectory      = halfsOpenDirectory      log
             , fuseReadDirectory      = halfsReadDirectory      log
             , fuseGetFileSystemStats = halfsGetFileSystemStats log
             }
  where
    log s = hPutStrLn logH s >> hFlush logH
    -- log _ = return ()

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
          -> IO (Either Errno ByteString)
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
