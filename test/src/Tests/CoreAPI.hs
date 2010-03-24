{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.CoreAPI
  (
   qcProps
  )
where

import Control.Concurrent
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

-- import Debug.Trace


--------------------------------------------------------------------------------
-- CoreAPI properties

-- Just dummy constructors for "this was the failing data sample" messages
newtype FileWR a        = FileWR a deriving Show
newtype DirMutexOk a    = DirMutexOk a deriving Show

type HalfsProp = 
  HalfsCapable b t r l m => BDGeom -> BlockDevice m -> PropertyM m ()

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [ exec 10  "Init and mount"         propM_initAndMountOK
  ,
    exec 10  "Mount/unmount"          propM_mountUnmountOK
  ,
    exec 10  "Unmount mutex"          propM_unmountMutexOK
  ,
    exec 10  "Directory construction" propM_dirConstructionOK
  ,
    exec 10  "Simple file creation"   propM_fileBasicsOK
  ,
    exec 100 "Simple file ops"        propM_simpleFileOpsOK
  ,
    exec 5   "File WR 1"              (propM_fileWROK "myfile")
  ,
    exec 5   "File WR 2"              (propM_fileWROK "foo/bar/baz")
  ,
    exec 50  "Directory mutex"        propM_dirMutexOK
  ,
    exec 10  "Hardlink creation"      propM_hardlinksOK
  ]
  where
    exec = mkMemDevExec quick "CoreAPI"


--------------------------------------------------------------------------------
-- Property Implementations

propM_initAndMountOK :: HalfsProp
propM_initAndMountOK _g dev = do
  esb <- runH (mkNewFS dev)
  case esb of
    Left _   -> fail "Filesystem creation failed"
    Right sb -> do 
      assert $ 1 == version sb
      assert $ unmountClean sb
      assert $ IN.nilInodeRef /= rootDir sb
    
      -- Mount the filesystem & ensure integrity with newfs contents
      runH (mount dev defaultUser defaultGroup) >>= \efs -> case efs of 
        Left  err -> fail $ "Mount failed: " ++ show err
        Right fs  -> do
          sb'      <- sreadRef (hsSuperBlock fs)
          freeBlks <- sreadRef (bmNumFree $ hsBlockMap fs)
          assert $ not $ unmountClean sb'
          assert $ sb' == sb{ unmountClean = False }
          assert $ freeBlocks sb' == freeBlks
    
      -- Mount the filesystem again and ensure "dirty unmount" failure
      expectErr (== HE_MountFailed DirtyUnmount)
                "Mount of unclean FS succeeded"
                (mount dev defaultUser defaultGroup)

      -- Corrupt the superblock and ensure "bad superblock" failure
      run $ bdWriteBlock dev 0 $ BS.replicate (fromIntegral $ bdBlockSize dev) 0
      expectErr pr "Mount of FS with corrupt SB succeeded"
                (mount dev defaultUser defaultGroup)
        where pr (HE_MountFailed BadSuperBlock{}) = True
              pr _                                = False

propM_mountUnmountOK :: HalfsProp
propM_mountUnmountOK _g dev = do
  fs <- runH (mkNewFS dev) >> mountOK dev
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
    unmountOK fs'           -- Unmount #1
    expectErr (== HE_UnmountFailed)
              "Two successive unmounts succeeded"
              (unmount fs') -- Unmount #2

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
  fs <- runH (mkNewFS dev) >> mountOK dev
  ch <- run newChan
  run $ forkIO $ threadTest fs ch
  run $ threadTest fs ch
  r1 <- run $ readChan ch
  r2 <- run $ readChan ch
  assert $ (r1 || r2) && not (r1 && r2) -- r1 `xor` r2
  where
    threadTest fs ch =
      runHalfs (unmount fs) >>=
        either (\_ -> writeChan ch False) (\_ -> writeChan ch True)

-- | Ensure that a new filesystem has the expected root directory intact
-- and that dirs and subdirs can be created and opened.
propM_dirConstructionOK :: HalfsProp
propM_dirConstructionOK _g dev = do
  fs <- runH (mkNewFS dev) >> mountOK dev

  -- Check that the root directory is present and contains only the initial
  -- directory entries.
  exec "openDir /" (openDir fs rootPath) >>= \dh -> do
    assert =<< (== Clean) `fmap` sreadRef (dhState dh)
    assert =<< ((== initDirEntNames) . L.sort . map deName . M.elems)
               `fmap` sreadRef (dhContents dh)
    
  -- TODO: replace this with a random valid hierarchy
  let p0 = rootPath </> "foo"
      p1 = p0 </> "bar"
      p2 = p1 </> "xxx"
      p3 = p2 </> "yyy"
      p4 = p0 </> "baz"
      p5 = p4 </> "zzz"

  mapM_ (test fs) [p0,p1,p2,p3,p4,p5]
  quickRemountCheck fs

  where
    exec = execH "propM_dirConstructionOK"
    --
    test fs p = do
      exec ("mkdir " ++ p) (mkdir fs p perms)
      isEmpty fs =<< exec ("openDir " ++ p) (openDir fs p)
    -- 
    isEmpty fs dh = do
      assert =<< null `fmap` exec ("readDir") (readDir fs dh)

-- | Ensure that a new filesystem populated with a handful of
-- directories permits creation/open/close of a file; also checks open
-- modes.
propM_fileBasicsOK :: HalfsProp
propM_fileBasicsOK _g dev = do
  fs <- runH (mkNewFS dev) >> mountOK dev
  
  let fooPath = rootPath </> "foo"
      fp      = fooPath </> "f1"
      fp2     = fooPath </> "f2"

  exec "mkdir /foo"          $ mkdir fs fooPath perms
  exec "create /foo/f1"      $ createFile fs fp defaultFilePerms
  fh0 <- exec "open /foo/f1" $ openFile fs fp fofReadOnly
  exec "close /foo/f1"       $ closeFile fs fh0

  expectErr (== HE_ObjectExists fp)
            "createFile on existing file succeeded"
            (createFile fs fp defaultFilePerms)

  -- Check open mode read/write permissiveness: can't read write-only,
  -- can't write read-only.
  exec "create /foo/f2"             $ createFile fs fp2 defaultFilePerms
  fh1 <- exec "rdonly open /foo/f1" $ openFile fs fp2 fofReadOnly
  expectErr (== HE_ErrnoAnnotated HE_BadFileHandleForWrite eBADF)
            "Write to read-only file succeeded"
            (write fs fh1 0 $ BS.singleton 0)
  exec "close /foo/f2" $ closeFile fs fh1

  fh2 <- exec "wronly open /foo/f1" $ openFile fs fp2 (fofWriteOnly False)
  expectErr (== HE_ErrnoAnnotated HE_BadFileHandleForRead eBADF)
            "Read from write-only file succeeded"
            (read fs fh2 0 0)

  quickRemountCheck fs
  where
    exec = execH "propM_fileBasicsOK"

-- | Ensure that simple write/readback works for a new file in a new
-- filesystem
propM_fileWROK :: FilePath -> HalfsProp
propM_fileWROK pathFromRoot _g dev = do
  fs <- runH (mkNewFS dev) >> mountOK dev

  assert (isRelative pathFromRoot)
  mapM_ (exec "making parent dir" . flip (mkdir fs) defaultDirPerms) mkdirs

  exec "create file"     $ createFile fs thePath defaultFilePerms
  fh <- exec "open file" $ openFile fs thePath (fofReadWrite False)

  forAllM (FileWR `fmap` choose (1, maxBytes)) $ \(FileWR fileSz) -> do
  forAllM (printableBytes fileSz)              $ \fileData        -> do 

  (_, _, api, apc) <- exec "Obtaining sizes" $ IN.getSizes (bdBlockSize dev)
  let expBlks = calcExpBlockCount (bdBlockSize dev) api apc fileSz

  let checkFileStat' atp mtp = do
        usr <- exec "get grp"    $ getUser fs
        grp <- exec "get usr"    $ getGroup fs
        st  <- exec "fstat file" $ fstat fs thePath
        checkFileStat st fileSz RegularFile defaultFilePerms
                      usr grp expBlks atp mtp

  t1 <- time
  exec "bytes -> file" $ write fs fh 0 fileData
  checkFileStat' (t1 <) (t1 <)

  -- Readback and check before closing the file
  t2 <- time
  fileData' <- exec "bytes <- file" $ read fs fh 0 (fromIntegral fileSz)
  checkFileStat' (t2 <) (t2 >)
  assrt "File readback OK" $ fileData == fileData'

  exec "close file" $ closeFile fs fh

  -- Reacquire the FH, read and check
  fh' <- exec "reopen file" $ openFile fs thePath fofReadOnly
  fileData'' <- exec "bytes <- file 2" $ read fs fh' 0 (fromIntegral fileSz)
  checkFileStat' (t2 <) (t2 >)
  assrt "Reopened file readback OK" $ fileData == fileData''

  quickRemountCheck fs
  where
    pathComps = splitDirectories $ fst $ splitFileName pathFromRoot
    mkdirs    = drop 1 $ scanl (\l r -> l ++ [pathSeparator] ++ r) "" pathComps
    thePath   = rootPath </> pathFromRoot
    time      = exec "obtain time" getTime
    exec      = execH "propM_fileWROK"
    assrt     = assertMsg "propM_fileWROK"
    maxBytes  = fromIntegral $ bdBlockSize dev * bdNumBlocks dev `div` 8

-- | Ensure that simple file operations (e.g., setFileSize) are
-- working correctly.

propM_simpleFileOpsOK :: HalfsProp
propM_simpleFileOpsOK _g dev = do
  fs <- runH (mkNewFS dev) >> mountOK dev
--  let fileSz = 30159
--      resizedSz = 36961 
--   let fileSz    = 1000
--       resizedSz = 1000 + 6802
--   let fileSz    = 512
--       resizedSz = 513
  forAllM (choose (1, maxBytes))        $ \fileSz    -> do
  forAllM (choose (0::Int, 2 * fileSz)) $ \resizedSz -> do 
  forAllM (printableBytes fileSz)       $ \fileData  -> do
                                 
--   trace ("propM_simpleFileOpsOK entry length fileData = " ++ show (BS.length fileData)) $ do

  let fp = rootPath </> "foo"
  exec "create /foo"      $ createFile fs fp defaultFilePerms
  fh0 <- exec "open /foo" $ openFile fs fp (fofWriteOnly True)
--   trace ("writing fileData") $ do
  exec "bytes -> file"    $ write fs fh0 0 fileData
--   trace ("wrote fileData") $ do
  exec "close /foo"       $ closeFile fs fh0
--   trace ("calling setFileSize") $ do
  exec "resize /foo"      $ setFileSize fs fp (fromIntegral resizedSz)
--   trace ("returned from setFileSize") $ do

  -- Check fstat
  newSz <- exec "fstat /foo"  $ fsSize `fmap` fstat fs fp
  assert (newSz == fromIntegral resizedSz)
--   trace ("newSz = " ++ show newSz) $ do
 
  -- Check contents
  fh1       <- exec "reopen /foo" $ openFile fs fp fofReadOnly
  fileData' <- exec "read /foo" $ read fs fh1 0 newSz
--   trace "here0" $ do
  assert (newSz == fromIntegral (BS.length fileData'))
--   trace "here1" $ do
  if resizedSz < fileSz
   then assert (fileData' == BS.take resizedSz fileData)
   else
--      trace ("length fileData = " ++ show (BS.length fileData) ++ ", length fileData' = " ++ show (BS.length fileData')) $ do
     assert (fileData' == fileData
                         `BS.append`
                         BS.replicate (resizedSz - fileSz) 0
            )
  exec "close /foo" $ closeFile fs fh1

--   trace "here2" $ do
  quickRemountCheck fs
  where
    maxBytes = fromIntegral $ bdBlockSize dev * bdNumBlocks dev `div` 8
    exec     = execH "propM_simpleFileOpsOK"

-- | Sanity check: multiple threads making many new directories
-- simultaneously are interleaved in a sensible manner.
propM_dirMutexOK :: BDGeom
                 -> BlockDevice IO
                 -> PropertyM IO ()
propM_dirMutexOK _g dev = do
  fs <- runH (mkNewFS dev) >> mountOK dev

  forAllM (DirMutexOk `fmap` choose (2, maxThreads)) $ \(DirMutexOk n) -> do
  forAllM (mapM genNm [1..n])                        $ \nmss           -> do

  ch <- run newChan
  mapM_ (run . forkIO . uncurry (threadTest fs ch)) (nmss `zip` [1..n])
  -- ^ n threads attempting to create directories simultaneously in /
  replicateM n (run $ readChan ch)
  -- ^ barrier

  dh               <- exec "openDir /" $ openDir fs "/"
  (dnames, dstats) <- exec "readDir /" $ unzip `fmap` readDir fs dh

  assertMsg "propM_dirMutexOK" "Directory contents are coherent" $
    L.sort dnames == L.sort (concat nmss ++ initDirEntNames)

  -- Misc extra check; convenient to do it here
  assertMsg "propM_dirMutexOK" "FileStats report Directory type" $
    all (== Directory) $ map fsType dstats

  quickRemountCheck fs 
  where
    maxDirs    = 50 -- } v
    maxLen     = 40 -- } arbitrary but fit into small devices
    maxThreads = 8
    exec       = execH "propM_dirMutexOK"
    ng f       = L.nub `fmap` resize maxDirs (listOf1 $ f maxLen)
    -- 
    genNm :: Int -> Gen [String]
    genNm n = map ((++) ("f" ++ show n ++ "_")) `fmap` ng filename
    -- 
    threadTest fs ch nms _n = do
      runHalfs $ mapM_ (flip (mkdir fs) perms . (</>) rootPath) nms
      writeChan ch ()

propM_hardlinksOK :: HalfsProp
propM_hardlinksOK _g dev = do
  -- Create a dummy filesystem with the following hierarchy
  -- / (directory)
  --   foo (directory)
  --     bar (directory)
  --       source (1 byte file)
  
  fs <- runH (mkNewFS dev) >> mountOK dev
  exec "mkdir /foo"                 $ mkdir fs d0 defaultDirPerms
  exec "mkdir /foo/bar"             $ mkdir fs d1 defaultDirPerms
  exec "creat /foo/bar/source"      $ createFile fs src defaultFilePerms
  fh <- exec "open /foo/bar/source" $ openFile fs src (fofWriteOnly True)
  forAllM (choose (1, maxBytes))    $ \fileSz   -> do
  forAllM (printableBytes fileSz)   $ \srcBytes -> do 
  exec "write /foo/bar/source"      $ write fs fh 0 srcBytes
  exec "close /foo/bar/source"      $ closeFile fs fh

  -- Expected error: File named by path1 is a directory
  expectErrno ePERM   =<< runH (mklink fs d1 dst1)
  -- Expected error: A component of path1's prefix is not a directory
  expectErrno eNOTDIR =<< runH (mklink fs (src </> "source") dst1)
  -- Expected error: A component of path1's prefix does not exist
  expectErrno eNOENT  =<< runH (mklink fs (d1 </> "bad" </> "source") dst1)
  -- Expected error: The file named by path1 does not exist
  expectErrno eNOENT  =<< runH (mklink fs (d1 </> "src2") dst1)
  -- Expected error: A component of path2's prefix is not a directory
  expectErrno eNOTDIR =<< runH (mklink fs src (src </> "dst1"))
  -- Expected error: A component of path 2's prefix does not exist
  expectErrno eNOENT  =<< runH (mklink fs src (d0 </> "bad" </> "bar"))
       
  mapM_ (\dst -> exec "mklink" $ mklink fs src dst) dests

  fhs <- mapM (\nm -> exec "open dst" $ openFile fs nm fofReadOnly) dests
  ds  <- mapM (\dh -> exec "read dst" $ read fs dh 0 (fromIntegral fileSz)) fhs

  assert $ all (== srcBytes) ds

  mapM (\dh -> exec "close dst" $ closeFile fs dh) fhs

  -- TODO: Test rmlink here

  quickRemountCheck fs
  where
    exec           = execH "propM_hardlinksOK"
    d0             = rootPath </> "foo"
    d1             = d0 </> "bar"
    src            = d1 </> "source"
    dests@(dst1:_) = [ d1 </> "link_to_source"
                     , d1 </> "link_2_to_source"
                     , d1 </> "link_3_to_source"
                     ]
    maxBytes       = fromIntegral $ bdBlockSize dev * bdNumBlocks dev `div` 8
    --
    expectErrno exp (Left (HE_ErrnoAnnotated _ errno)) = assert (errno == exp)
    expectErrno _ _                                    = assert False


--------------------------------------------------------------------------------
-- Misc

initDirEntNames :: [FilePath]
initDirEntNames = [dotPath, dotdotPath]

perms :: FileMode
perms = FileMode [Read,Write,Execute] [Read,Execute] [Read,Execute]

rootPath :: FilePath
rootPath = [pathSeparator]

-- Somewhat lightweight sanity check that unmounts/remounts and does a
-- hacky "equality" check on filesystem contents.  We defer to other
-- tests to more adequately test deep structural equality post-remount.
quickRemountCheck :: HalfsCapable b t r l m =>
                     HalfsState b r l m
                  -> PropertyM m ()
quickRemountCheck fs = do
  dump0 <- exec "Get dump0" $ dumpfs fs
  unmountOK fs
  fs'   <- mountOK (hsBlockDev fs)
  dump1 <- exec "Get dump1" $ dumpfs fs'
  assert (dump0 == dump1)
  unmountOK fs'
  where
    exec = execH "quickRemountCheck"
