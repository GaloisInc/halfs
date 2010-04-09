{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.CoreAPI
  (
   qcProps
  )
where

import Control.Concurrent
import Data.Either
import Data.List
import Data.Maybe
import Data.Serialize
import Foreign.C.Error 
import Prelude hiding (read, exp)
import System.FilePath
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic 
import qualified Data.ByteString as BS
import qualified Data.List as L
import qualified Data.Map as M

import Halfs.BlockMap
import Halfs.Classes
import Halfs.CoreAPI
import Halfs.Errors
import Halfs.File hiding (createFile)
import Halfs.HalfsState
import qualified Halfs.Inode as IN
import Halfs.Monad
import Halfs.SuperBlock
import Halfs.Types

import System.Device.BlockDevice (BlockDevice(..))

import Tests.Instances (printableBytes, filename)
import Tests.Types
import Tests.Utils

import Debug.Trace


--------------------------------------------------------------------------------
-- CoreAPI properties

-- Just dummy constructors for "this was the failing data sample" messages
newtype FileWR a        = FileWR a deriving Show
newtype DirMutexOk a    = DirMutexOk a deriving Show

type HalfsProp = 
  HalfsCapable b t r l m => BDGeom -> BlockDevice m -> PropertyM m ()

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [
--     exec 10 "Init and mount"         propM_initAndMountOK
--   ,
--     exec 10 "Mount/unmount"          propM_mountUnmountOK
--   ,
--     exec 10 "Unmount mutex"          propM_unmountMutexOK
--   ,
--     exec 10 "Directory construction" propM_dirConstructionOK
--   ,
--     exec 10 "Simple file creation"   propM_fileBasicsOK
--   ,
--     exec 10 "Simple file ops"        propM_simpleFileOpsOK
--   ,
--     exec 10 "chmod/chown ops"        propM_chmodchownOK
--   ,
--     exec 5  "File WR 1"              (propM_fileWROK "myfile")
--   ,
--     exec 5  "File WR 2"              (propM_fileWROK "foo/bar/baz")
--   ,
--     exec 10 "Directory mutex"        propM_dirMutexOK
--   ,
--     exec 10 "Hardlink creation"      propM_hardlinksOK
--   ,
--     exec 10 "Simple rmdir"           propM_simpleRmdirOK
--   ,
--     exec 10 "rmdir mutex"            propM_rmdirMutexOK
--   ,
--     exec 10 "Simple rmlink"          propM_simpleRmlinkOK
--  ,
    exec 1 "Simple rename"           propM_simpleRenameOK
  ]
  where
    exec = mkMemDevExec quick "CoreAPI"


--------------------------------------------------------------------------------
-- Property Implementations

propM_initAndMountOK :: HalfsProp
propM_initAndMountOK _g dev = do
  esb <- mkNewFS dev
  case esb of
    Left _   -> fail "Filesystem creation failed"
    Right sb -> do 
      assert $ 1 == version sb
      assert $ unmountClean sb
      assert $ IN.nilInodeRef /= rootDir sb
    
      -- Mount the filesystem & ensure integrity with newfs contents
      runHNoEnv (mount dev defaultUser defaultGroup) >>= \efs -> case efs of 
        Left  err -> fail $ "Mount failed: " ++ show err
        Right fs  -> do
          sb'      <- sreadRef (hsSuperBlock fs)
          freeBlks <- sreadRef (bmNumFree $ hsBlockMap fs)
          assert $ not $ unmountClean sb'
          assert $ sb' == sb{ unmountClean = False }
          assert $ freeBlocks sb' == freeBlks
    
      -- Mount the filesystem again and ensure "dirty unmount" failure
      e0 <- runHNoEnv (mount dev defaultUser defaultGroup)
      case e0 of
        Left (HE_MountFailed DirtyUnmount) -> return ()
        _                                  -> assert False

      -- Corrupt the superblock and ensure "bad superblock" failure
      run $ bdWriteBlock dev 0 $ BS.replicate (fromIntegral $ bdBlockSize dev) 0
      e1 <- runHNoEnv (mount dev defaultUser defaultGroup)
      case e1 of
        Left (HE_MountFailed BadSuperBlock{}) -> return ()
        _                                     -> assert False
                                               
propM_mountUnmountOK :: HalfsProp
propM_mountUnmountOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev
  let readSBRef = sreadRef $ hsSuperBlock fs

  assert =<< (not . unmountClean) `fmap` readSBRef
  unmountOK fs

  -- Check the uncleanMount flag in both the in-memory and persisted superblock
  assert =<< unmountClean `fmap` readSBRef
  assert =<< unmountClean `fmap`
             (run (bdReadBlock dev 0) >>= either fail return . decode)

  -- Ensure that an additional mount/unmount sequence is successful
  mountOK dev >>= unmountOK

  -- Ensure that double unmount results in an error
  mountOK dev >>= \fs' -> do
    unmountOK fs' -- Unmount #1
    e0 <- runH fs' unmount
    case e0 of
      Left HE_UnmountFailed -> return ()
      _                     -> assert False     

-- | Sanity check: unmount is mutex'd sensibly

-- NB: this is probably of limited utility at the time of writing, because
-- removing the locks from the unmount still yields the expected behavior; doing
-- gymnastics to ensure various kinds of interleaving is probably not worth it
-- for this small test.  This might change down the road, though, as the unmount
-- responsibilities take longer.  Also, something like this is probably going to
-- be useful for multithreaded tests (and for operations more frequent than
-- unmount) down the road, so we'll leave it here for the time being.
propM_unmountMutexOK :: BDGeom
                     -> BlockDevice IO
                     -> PropertyM IO ()
propM_unmountMutexOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev
  ch <- run newChan
  run $ forkIO $ threadTest fs ch
  run $ threadTest fs ch
  r1 <- run $ readChan ch
  r2 <- run $ readChan ch
  assert $ (r1 || r2) && not (r1 && r2) -- r1 `xor` r2
  where
    threadTest fs ch =
      runHalfs fs unmount >>=
        either (\_ -> writeChan ch False) (\_ -> writeChan ch True)

-- | Ensure that a new filesystem has the expected root directory intact
-- and that dirs and subdirs can be created and opened.
propM_dirConstructionOK :: HalfsProp
propM_dirConstructionOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev
  let exec = execH "propM_dirConstructionOK" fs
      --
      test p = do
        exec ("mkdir " ++ p) (mkdir p defaultDirPerms)
        isEmpty =<< exec ("openDir " ++ p) (openDir p)
      -- 
      isEmpty dh = 
        assert =<< do dnames <- map fst `fmap` exec "readDir" (readDir dh)
                      return $ L.sort dnames == L.sort initDirEntNames

  -- Check that the root directory is present and empty (. and .. are
  -- implicit via readDir)
  exec "openDir /" (openDir rootPath) >>= \dh -> do
    assert =<< (== Clean) `fmap` sreadRef (dhState dh)
    assert =<< M.null `fmap` sreadRef (dhContents dh)
    
  -- TODO: replace this with a random valid hierarchy
  let p0 = rootPath </> "foo"
      p1 = p0 </> "bar"
      p2 = p1 </> "xxx"
      p3 = p2 </> "yyy"
      p4 = p0 </> "baz"
      p5 = p4 </> "zzz"

  mapM_ test [p0,p1,p2,p3,p4,p5]
  quickRemountCheck fs

-- | Ensure that a new filesystem populated with a handful of
-- directories permits creation/open/close of a file; also checks open
-- modes.
propM_fileBasicsOK :: HalfsProp
propM_fileBasicsOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev
  let exec           = execH "propM_fileBasicsOK" fs
      fooPath        = rootPath </> "foo"
      fp             = fooPath </> "f1"
      fp2            = fooPath </> "f2"


  exec "mkdir /foo"          $ mkdir fooPath defaultDirPerms
  exec "create /foo/f1"      $ createFile fp defaultFilePerms
  fh0 <- exec "open /foo/f1" $ openFile fp fofReadOnly
  exec "close /foo/f1"       $ closeFile fh0

  e0 <- runH fs $ createFile fp defaultFilePerms
  case e0 of
    Left (HE_ObjectExists fp') -> assert (fp == fp')
    _                          -> assert False

  -- Check open mode read/write permissiveness: can't read write-only,
  -- can't write read-only.
  exec "create /foo/f2"             $ createFile fp2 defaultFilePerms
  fh1 <- exec "rdonly open /foo/f1" $ openFile fp2 fofReadOnly

  -- Expect a write to a read-only file to fail
  e1 <- runH fs $ write fh1 0 $ BS.singleton 0
  case e1 of
    Left (HE_ErrnoAnnotated HE_BadFileHandleForWrite errno) ->
      assert (errno == eBADF)
    _ -> assert False
  exec "close /foo/f2" $ closeFile fh1

  fh2 <- exec "wronly open /foo/f1" $ openFile fp2 (fofWriteOnly False)
  -- Expect a read from a write-only file to fail
  e2 <- runH fs $ read fh2 0 0
  case e2 of
    Left (HE_ErrnoAnnotated HE_BadFileHandleForRead errno) ->
      assert (errno == eBADF)
    _ -> assert False

  quickRemountCheck fs

-- | Ensure that simple write/readback works for a new file in a new
-- filesystem
propM_fileWROK :: FilePath -> HalfsProp
propM_fileWROK pathFromRoot _g dev = do
  fs <- mkNewFS dev >> mountOK dev
  let exec = execH "propM_fileWROK" fs
      time = exec "obtain time" getTime

  assert (isRelative pathFromRoot)
  mapM_ (exec "making parent dir" . flip mkdir defaultDirPerms) mkdirs

  exec "create file"     $ createFile thePath defaultFilePerms
  fh <- exec "open file" $ openFile thePath (fofReadWrite False)

  forAllM (FileWR `fmap` choose (1, maxBytes)) $ \(FileWR fileSz) -> do
  forAllM (printableBytes fileSz)              $ \fileData        -> do 

  (_, _, api, apc) <- exec "Obtaining sizes" $ IN.getSizes (bdBlockSize dev)
  let expBlks = calcExpBlockCount (bdBlockSize dev) api apc fileSz

  let checkFileStat' atp mtp ctp = do
        usr <- exec "get grp" getUser
        grp <- exec "get usr" getGroup
        st  <- exec "fstat file" $ fstat thePath
        checkFileStat st fileSz RegularFile defaultFilePerms
                      usr grp expBlks atp mtp ctp

  t1 <- time
  exec "bytes -> file" $ write fh 0 fileData
  checkFileStat' (t1 <=) (t1 <=) (t1 <=)

  -- Readback and check before closing the file
  t2 <- time
  fileData' <- exec "bytes <- file" $ read fh 0 (fromIntegral fileSz)
  checkFileStat' (t2 <=) (t2 >=) (t2 <=)
  assrt "File readback OK" $ fileData == fileData'

  exec "close file" $ closeFile fh

  -- Reacquire the FH, read and check
  fh' <- exec "reopen file" $ openFile thePath fofReadOnly
  fileData'' <- exec "bytes <- file 2" $ read fh' 0 (fromIntegral fileSz)
  checkFileStat' (t2 <=) (t2 >=) (t2 <=)
  assrt "Reopened file readback OK" $ fileData == fileData''

  quickRemountCheck fs
  where
    pathComps = splitDirectories $ fst $ splitFileName pathFromRoot
    mkdirs    = drop 1 $ scanl (\l r -> l ++ [pathSeparator] ++ r) "" pathComps
    thePath   = rootPath </> pathFromRoot
    assrt     = assertMsg "propM_fileWROK"
    maxBytes  = fromIntegral $ bdBlockSize dev * bdNumBlocks dev `div` 8

-- | Ensure that simple file operations (e.g., setFileSize) are
-- working correctly.
propM_simpleFileOpsOK :: HalfsProp
propM_simpleFileOpsOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev
  let exec = execH "propM_simpleFileOpsOK" fs

  forAllM (choose (1, maxBytes))        $ \fileSz    -> do
  forAllM (choose (0::Int, 2 * fileSz)) $ \resizedSz -> do 
  forAllM (printableBytes fileSz)       $ \fileData  -> do
                                 
  let fp = rootPath </> "foo"
  exec "create /foo"      $ createFile fp defaultFilePerms
  fh0 <- exec "open /foo" $ openFile fp (fofWriteOnly True)
  exec "bytes -> file"    $ write fh0 0 fileData
  exec "close /foo"       $ closeFile fh0
  exec "resize /foo"      $ setFileSize fp (fromIntegral resizedSz)

  -- Check that fstat is coherent
  st <- exec "fstat /foo" $ fstat fp
  let newSz = fsSize st
  assert (newSz == fromIntegral resizedSz)
 
  -- Check that file contents have truncated/grown as expected
  fh1       <- exec "reopen /foo" $ openFile fp fofReadOnly
  fileData' <- exec "read /foo" $ read fh1 0 newSz
  assert (newSz == fromIntegral (BS.length fileData'))
  if resizedSz < fileSz
   then assert (fileData' == BS.take resizedSz fileData)
   else
     assert (fileData' == fileData
                         `BS.append`
                         BS.replicate (resizedSz - fileSz) 0
            )
  exec "close /foo" $ closeFile fh1

  -- Check setFileTimes
  now <- exec "get time" getTime
  exec "setFileTimes" $ setFileTimes fp now now
  st' <- exec "fstat /foo" $ fstat fp

{-
  trace ("now = " ++ show now) $ do
  trace ("fsAccessTime st' = " ++ show (fsAccessTime st')) $ do
  trace ("fsModifyTime st' = " ++ show (fsModifyTime st')) $ do
  trace ("fsChangeTime st' = " ++ show (fsChangeTime st')) $ do
-}

  -- NB: May need to have define a "close enough" equivalence function for
  -- comparing times here, due to the second granularity of the time sampling
  -- and truncation in the current implementation of getTime.
  assert (fsAccessTime st' == now)
  assert (fsModifyTime st' == now)
  assert (fsChangeTime st' >= now)

  quickRemountCheck fs
  where
    maxBytes = fromIntegral $ bdBlockSize dev * bdNumBlocks dev `div` 8

-- | Ensure that simple file operations (e.g., setFileSize) are
-- working correctly.
propM_chmodchownOK :: HalfsProp
propM_chmodchownOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev

  let exec = execH "propM_chmodchownOK" fs
      fp   = rootPath </> "foo"

  exec "create /foo" $ createFile fp defaultFilePerms

  -- chmod to a random and check
  forAllM arbitrary $ \perms -> do 
  exec "chmod 600 /foo"    $ chmod fp perms
  st0 <- exec "fstat /foo" $ fstat fp
  assert (fsMode st0 == perms)

  -- chown/chgrp to random uid/gid and check
  forAllM arbitrary $ \usr -> do
  forAllM arbitrary $ \grp -> do                             
  exec "ch{own,grp} /foo"  $ chown fp (Just usr) (Just grp)
  st1 <- exec "fstat /foo" $ fstat fp
  assert (fsUID st1 == usr)
  assert (fsGID st1 == grp)

  -- No change variants
  forAllM arbitrary $ \usr2 -> do
  forAllM arbitrary $ \grp2 -> do
  exec "chown /foo" $ chown fp (Just usr2) Nothing -- don't change group
  st2 <- exec "fstat /foo" $ fstat fp
  assert (fsUID st2 == usr2)
  assert (fsGID st2 == grp)

  exec "chgrp /foo" $ chown fp Nothing (Just grp2) -- don't change user
  st3 <- exec "fstat /foo" $ fstat fp
  assert (fsUID st3 == usr2)
  assert (fsGID st3 == grp2)

  quickRemountCheck fs

-- | Sanity check: multiple threads making many new directories
-- simultaneously are interleaved in a sensible manner.
propM_dirMutexOK :: BDGeom
                 -> BlockDevice IO
                 -> PropertyM IO ()
propM_dirMutexOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev
  let exec = execH "propM_dirMutexOK" fs

  forAllM (DirMutexOk `fmap` choose (8, maxThreads)) $ \(DirMutexOk n) -> do
  forAllM (mapM genNm [1..n])                        $ \nmss           -> do

  ch <- run newChan
  mapM_ (run . forkIO . uncurry (threadTest fs ch)) (nmss `zip` [1..n])
  -- ^ n threads attempting to create directories simultaneously in /
  exceptions <- catMaybes `fmap` replicateM n (run $ readChan ch)
  -- ^ barrier with exceptions aggregated from each thread
  assrt ("Caught exception(s) from thread(s): " ++ show exceptions)
        (null exceptions)

  dh               <- exec "openDir /" $ openDir "/"
  (dnames, dstats) <- exec "readDir /" $ unzip `fmap` readDir dh

  let p = L.sort dnames; q = L.sort $ concat nmss ++ initDirEntNames
  assrt ("Directory contents incoherent (found discrepancy: "
          ++ show (nub $ (p \\ q) ++ (q \\ p)) ++ ")"
        )
        (p == q)

  -- Misc extra check; convenient to do it here
  assrt "File stats reported non-Directory type" $
    all (== Directory) $ map fsType dstats

  quickRemountCheck fs 
  where
    assrt      = assertMsg "propM_dirMutexOK"
    maxDirs    = 50 -- } v
    maxLen     = 40 -- } arbitrary name length, but fit into small devices
    maxThreads = 8
    ng f       = L.nub `fmap` resize maxDirs (listOf1 $ f maxLen)
    -- 
    genNm :: Int -> Gen [String]
    genNm n = map ((++) ("f" ++ show n ++ "_")) `fmap` ng filename
    -- 
    threadTest fs ch nms _n = 
      runHalfs fs (mapM_ (flip mkdir defaultDirPerms . (</>) rootPath) nms)
        >>= writeChan ch . either Just (const Nothing)

propM_hardlinksOK :: HalfsProp
propM_hardlinksOK _g dev = do
  -- Create a dummy filesystem with the following hierarchy
  -- / (directory)
  --   foo (directory)
  --     bar (directory)
  --       source (1 byte file)
  
  fs <- mkNewFS dev >> mountOK dev
  let exec = execH "propM_hardlinksOK" fs
  exec "mkdir /foo"                 $ mkdir d0 defaultDirPerms
  exec "mkdir /foo/bar"             $ mkdir d1 defaultDirPerms
  exec "creat /foo/bar/source"      $ createFile src defaultFilePerms
  fh <- exec "open /foo/bar/source" $ openFile src (fofWriteOnly True)
  forAllM (choose (1, maxBytes))    $ \fileSz   -> do
  forAllM (printableBytes fileSz)   $ \srcBytes -> do 
  exec "write /foo/bar/source"      $ write fh 0 srcBytes
  exec "close /foo/bar/source"      $ closeFile fh

  -- Expected error: File named by path1 is a directory
  expectErrno ePERM   =<< runH fs (mklink d1 dst1)
  -- Expected error: A component of path1's prefix is not a directory
  expectErrno eNOTDIR =<< runH fs (mklink (src </> "source") dst1)
  -- Expected error: A component of path1's prefix does not exist
  expectErrno eNOENT  =<< runH fs (mklink (d1 </> "bad" </> "source") dst1)
  -- Expected error: The file named by path1 does not exist
  expectErrno eNOENT  =<< runH fs (mklink (d1 </> "src2") dst1)
  -- Expected error: A component of path2's prefix is not a directory
  expectErrno eNOTDIR =<< runH fs (mklink src (src </> "dst1"))
  -- Expected error: A component of path 2's prefix does not exist
  expectErrno eNOENT  =<< runH fs (mklink src (d0 </> "bad" </> "bar"))
       
  mapM_ (\dst -> exec "mklink" $ mklink src dst) dests

  fhs <- mapM (\nm -> exec "open dst" $ openFile nm fofReadOnly) dests
  ds  <- mapM (\dh -> exec "read dst" $ read dh 0 (fromIntegral fileSz)) fhs

  assert $ all (== srcBytes) ds

  mapM (exec "close dst" . closeFile) fhs

  -- TODO: Test rmlink here

  quickRemountCheck fs
  where
    d0             = rootPath </> "foo"
    d1             = d0 </> "bar"
    src            = d1 </> "source"
    dests@(dst1:_) = [ d1 </> "link_to_source"
                     , d1 </> "link_2_to_source"
                     , d1 </> "link_3_to_source"
                     ]
    maxBytes       = fromIntegral $ bdBlockSize dev * bdNumBlocks dev `div` 8

propM_simpleRmdirOK :: HalfsProp
propM_simpleRmdirOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev
  let exec     = execH "propM_simpleRmdirOK" fs
      d0       = rootPath </> "foo"
      d1       = d0       </> "bar"
      fp       = d0       </> "f1"
      exists p =
        (elem (takeFileName p) . map fst)
          `fmap` exec ("readDir " ++ p)
                      (withDir (takeDirectory p) readDir)


  -- Expected error: removal of non-empty directory
  exec "mkdir /foo"     $ mkdir d0 defaultDirPerms
  exec "create /foo/f1" $ createFile fp defaultFilePerms
  e0 <- runH fs $ rmdir d0
  case e0 of
    Left (HE_ErrnoAnnotated HE_DirectoryNotEmpty errno) ->
      assert (errno == eNOTEMPTY)
    _ -> assert False

  -- simple mkdir/rmdir check
  freeBefore <- getFree fs
  exec "mkdir /foo/bar" $ mkdir d1 defaultDirPerms
  dh <- exec "openDir /foo/bar" $ openDir d1
  assert =<< exists d1
  exec "rmdir /foo/bar" $ rmdir d1
  assert =<< liftM2 (==) (getFree fs) (return freeBefore)
  assert =<< not `fmap` exists d1

  -- Expected error: no access to an invalidated directory handle
  e1 <- runH fs $ readDir dh
  case e1 of
    Left HE_InvalidDirHandle -> return ()
    _                        -> assert False
  quickRemountCheck fs 

-- Simple sanity check for the mutex behavior rmdir is supposed to be enforcing.
-- We have two threads, A and B.  Thread A repeatedly attempts to create a
-- directory and delete it.  Directory creation should never fail. Thread B
-- repeatedly attempts to readDir on the directory and and should fail only if
-- (1) the directory does not exist or (2) the directory handle has been
-- invalidated (due to A's removal of the directory).
--
-- This test can be beefed up considerably (e.g., by creating files into the
-- directory to give mkdir an acceptable failure case, etc.), but at the time
-- the test was written, file removal and so forth didn't exist
-- yet. Furthermore, this'll only catch bad/unexpected errors getting thrown and
-- will not necessarily catch, e.g., illegal writes going to the underlying
-- inode.
-- 
-- TODO: Revisit.
propM_rmdirMutexOK :: BDGeom
                   -> BlockDevice IO
                   -> PropertyM IO ()
propM_rmdirMutexOK _g dev = do
  fs  <- mkNewFS dev >> mountOK dev
  chA <- run newChan
  chB <- run newChan
  forAllM (choose (100, 1000) :: Gen Int) $ \numTrials -> do
  run $ forkIO $ threadA fs chA numTrials
  run $ forkIO $ threadB fs chB numTrials
  
  -- except only Left-constructed results from the worker threads
  assert =<< (null . rights) `fmap` replicateM numTrials (run $ readChan chA)
  assert =<< (null . rights) `fmap` replicateM numTrials (run $ readChan chB)
  -- ^ barriers
  
  quickRemountCheck fs 
  where
    dp = rootPath </> "theDir"
    --
    threadA fs ch n
      | n == 0    = return () -- writeChan ch ()
      | otherwise = do
          -- Thread A create directory, should never fail
          e0 <- runHalfs fs $ mkdir dp defaultDirPerms
          merr <- case e0 of
            Left e  -> return (Right e)
            Right _ -> runHalfs fs (rmdir dp)
                         >>= either (return . Right)
                                    (const $ return $ Left "ok")
          writeChan ch merr            
          threadA fs ch (n-1)
    -- 
    threadB fs ch n
      | n == 0    = return ()
      | otherwise = do
          -- ThreadB readDir, should only fail if dir DNE or the DH is invalid
          e0 <- runHalfs fs $ withDir dp readDir
          merr <- case e0 of
            Left HE_PathComponentNotFound{} -> return (Left "pcnf")
            Left HE_InvalidDirHandle        -> return (Left "idh")
            Left e                          -> return (Right e)
            _                               -> return (Left "ok")
          writeChan ch merr
          threadB fs ch (n-1)

propM_simpleRmlinkOK :: HalfsProp
propM_simpleRmlinkOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev

  let exec     = execH "propM_simpleRmlinkOK" fs
      f1       = rootPath </> "f1"
      f2       = rootPath </> "f2"
      exists p =
        (elem (takeFileName p) . map fst)
          `fmap` exec ("readDir " ++ p)
                      (withDir (takeDirectory p) readDir)

  freeBefore <- getFree fs
  exec "create /f1" $ createFile f1 defaultFilePerms
  assert =<< exists f1

  -- Expected error: no access to an invalidated filehandle
  deadFH <- exec "open /f1" $ openFile f1 fofReadOnly
  exec "close /f1"          $ closeFile deadFH
  e1 <- runH fs $ read deadFH 0 0
  case e1 of
    Left HE_InvalidFileHandle -> return ()
    _                         -> assert False

  exec "mklink /f1 /f2" $ mklink f1 f2
  assert =<< and `fmap` mapM exists [f1, f2]

  exec "rmlink /f1" $ rmlink f1
  assert =<< not `fmap` exists f1
  assert =<< exists f2

  exec "rmlink /f2" $ rmlink f2
  assert =<< not `fmap` exists f2

  freeAfter <- getFree fs
  assert (freeBefore >= freeAfter && freeBefore - freeAfter == 1)
  -- ^ 1 extra block still claimed for root directory contents

  quickRemountCheck fs         

propM_simpleRenameOK :: HalfsProp
propM_simpleRenameOK _g dev = do
  fs <- mkNewFS dev >> mountOK dev
  let exec      = execH "propM_simpleRenameOK" fs
      dne p     = assert =<< not `fmap` exists' p
      exists p  = assert =<< exists' p
      exists' p =
        (elem (takeFileName p) . map fst)
          `fmap` exec ("readDir " ++ p)
                      (withDir (takeDirectory p) readDir)
{-
  -- Expected error: Path component not found
  expectErrno eNOENT =<< runH fs (rename f1 f2)
-}
  trace ("f1 = " ++ show f1) $ do
  trace ("f3 = " ++ show f3) $ do
  exec "create /f1"      $ createFile f1 defaultFilePerms
  exec "create /f3"      $ createFile f3 defaultFilePerms
  exec "mkdirs"          $ mapM_ (`mkdir` defaultDirPerms) [d1, d1sub, d3]
  mapM exists [f1, f3, d1, d1sub, d3]

{-
  -- Expected error: Path component not found
  expectErrno eNOENT  =<< runH fs (rename f1 (rootPath </> "blah" </> "blah"))
  -- Expected error: new is a directory, but old is not a directory
  expectErrno eISDIR  =<< runH fs (rename f1 d1)
  -- Expected error: old is a directory, but new is not a directory
  expectErrno eNOTDIR =<< runH fs (rename d1 f1)
  -- Expected error: old is a parent directory of new
  expectErrno eINVAL  =<< runH fs (rename d1 d1sub)
  -- Expected error: attempt to rename '.' or '..'
  expectErrno eINVAL  =<< runH fs (rename dotPath d2)
  expectErrno eINVAL  =<< runH fs (rename dotdotPath d2)  
-}

  -- File to non-existent dest
  zeroOrMoreBlocksAllocd fs $ exec "rename /f1 /f2" $ rename f1 f2
  dne f1
  mapM exists [f2, f3, d1, d1sub, d3]

  -- File to existent dest
  blocksUnallocd 1 fs $ exec "rename /f2 /f3" $ rename f2 f3
  mapM dne [f1, f2]
  mapM exists [f3, d1, d1sub, d3]
  
  -- Directory to non-existent dest
  zeroOrMoreBlocksAllocd fs $ exec "rename /d1/d1sub /d2" $ rename d1sub d2
  mapM dne [f1, f2, d1sub]
  mapM exists [f3, d1, d2, d3]

  -- Directory to existing empty dest dir
  blocksUnallocd 1 fs $ exec "rename /d3 /d2" $ rename d3 d2
  mapM dne [f1, f2, d1sub, d3]
  mapM exists [f3, d1, d2]

  -- File into dir
  x <- getFree fs
  zeroOrMoreBlocksAllocd fs $ exec "rename /f3 /d2/f3" $ rename f3 (d2 </> "f3")
  y <- getFree fs
  trace ("x = " ++ show x ++ ", show y = " ++ show y) $ do
  mapM dne [f1, f2, d1sub, d3]
  mapM exists [d1, d2, d2 </> "f3"]

  -- Directory to existing non-empty dest dir
  zeroOrMoreBlocksAllocd fs $ expectErrno eNOTEMPTY =<< runH fs (rename d1 d2)
  quickRemountCheck fs
  return ()
  where
    -- 
    d1sub                    = d1 </> "d1sub"
    [d1, d2, d3, f1, f2, f3] =
      map (rootPath </>) ["d1", "d2", "d3", "f1", "f2", "f3"]



--------------------------------------------------------------------------------
-- Misc

-- Block utilization checking combinators
rscUtil :: HalfsCapable b t r l m =>
           (Word64 -> Word64 -> Bool) -- ^ predicate on after/before block cnts
        -> HalfsState b r l m         -- ^ the filesystem state
        -> PropertyM m a              -- ^ the action to check
        -> PropertyM m ()
rscUtil p fs act = do b <- getFree fs; act; a <- getFree fs; assert (p a b)

blocksUnallocd :: HalfsCapable b t r l m =>
                  Word64             -- ^ expected #blocks unallocated
               -> HalfsState b r l m -- ^ the filesystem state
               -> PropertyM m a      -- ^ the action to check
               -> PropertyM m ()
blocksUnallocd x = rscUtil (\a b -> a >= b && a - b == x)
                   
zeroOrMoreBlocksAllocd :: HalfsCapable b t r l m =>
                          HalfsState b r l m -- ^ the filesystem state
                       -> PropertyM m a      -- ^ the action to check
                       -> PropertyM m ()
zeroOrMoreBlocksAllocd = rscUtil (<=)

getFree :: (HalfsCapable b t r l m) => HalfsState b r l m -> PropertyM m Word64
getFree = sreadRef . bmNumFree . hsBlockMap

expectErrno :: Monad m => Errno -> Either HalfsError a -> PropertyM m ()
expectErrno exp (Left (HE_ErrnoAnnotated _ errno)) = assert (errno == exp)
expectErrno _ _                                    = assert False

initDirEntNames :: [FilePath]
initDirEntNames = [dotPath, dotdotPath]

rootPath :: FilePath
rootPath = [pathSeparator]

-- Somewhat lightweight sanity check that unmounts/remounts and does a hacky and
-- inefficient "equality" check on filesystem contents.  We defer to other tests
-- to more adequately test deep structural equality post-remount.
quickRemountCheck :: HalfsCapable b t r l m =>
                     HalfsState b r l m
                  -> PropertyM m ()
quickRemountCheck fs = do
  dump0 <- exec "Get dump0" dumpfs
  trace ("dump0: " ++ dump0) $ do 
  unmountOK fs
  fs'   <- mountOK (hsBlockDev fs)
  dump1 <- exec "Get dump1" dumpfs
  assert (dump0 == dump1)
  unmountOK fs'
  where
    exec = execH "quickRemountCheck" fs


