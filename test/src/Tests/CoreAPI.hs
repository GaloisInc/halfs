{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.CoreAPI
  (
   qcProps
  )
where

import Control.Concurrent
import Data.Serialize
import Prelude hiding (read)
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
import Halfs.HalfsState
import Halfs.Inode
import Halfs.Monad
import Halfs.SuperBlock
import Halfs.Types
import System.Device.BlockDevice (BlockDevice(..))

import Tests.Instances (printableBytes, filename)
import Tests.Types
import Tests.Utils


--------------------------------------------------------------------------------
-- CoreAPI properties

-- Just dummy constructors for "this was the failing data sample" messages
newtype FileWR a       = FileWR a deriving Show
newtype DirMutexOk a   = DirMutexOk a deriving Show


type HalfsProp = 
  HalfsCapable b t r l m => BDGeom -> BlockDevice m -> PropertyM m ()

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [
     exec 10  "Init and mount"         propM_initAndMountOK
   , exec 10 "Mount/unmount"           propM_mountUnmountOK
   , exec 10 "Unmount mutex"           propM_unmountMutexOK
   , exec 10  "Directory construction" propM_dirConstructionOK
   , exec 10 "Simple file creation"    propM_fileCreationOK
   , exec 10 "File WR"                 propM_fileWR
   , exec 50 "Directory mutex"         propM_dirMutexOK
  ]
  where
    exec = mkMemDevExec quick "CoreAPI"


--------------------------------------------------------------------------------
-- Property Implementations

propM_initAndMountOK :: HalfsProp
propM_initAndMountOK _g dev = do
  esb <- runH (newfs dev)
  case esb of
    Left _   -> fail "Filesystem creation failed"
    Right sb -> do 
      assert $ 1 == version sb
      assert $ unmountClean sb
      assert $ nilInodeRef /= rootDir sb
    
      -- Mount the filesystem & ensure integrity with newfs contents
      runH (mount dev) >>= \efs -> case efs of 
        Left  err -> fail $ "Mount failed: " ++ show err
        Right fs  -> do
          sb'      <- sreadRef (hsSuperBlock fs)
          freeBlks <- sreadRef (bmNumFree $ hsBlockMap fs)
          assert $ not $ unmountClean sb'
          assert $ sb' == sb{ unmountClean = False }
          assert $ freeBlocks sb' == freeBlks
    
      -- Mount the filesystem again and ensure "dirty unmount" failure
      runH (mount dev) >>= \efs -> case efs of
        Left (HalfsMountFailed DirtyUnmount) -> ok
        Left err                             -> unexpectedErr err
        Right _                              ->
          fail "Mount of unclean filesystem succeeded"
    
      -- Corrupt the superblock and ensure "bad superblock" failure
      run $ bdWriteBlock dev 0 $ BS.replicate (fromIntegral $ bdBlockSize dev) 0
      runH (mount dev) >>= \efs -> case efs of
        Left (HalfsMountFailed BadSuperBlock{}) -> ok
        Left err                                -> unexpectedErr err
        Right _                                 ->
          fail "Mount of filesystem with corrupt superblock succeeded"

propM_mountUnmountOK :: HalfsProp
propM_mountUnmountOK _g dev = do
  fs <- runH (newfs dev) >> mountOK dev
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
    unmountOK fs'                              -- Unmount #1
    runH (unmount fs') >>= \efs -> case efs of -- Unmount #2
      Left HalfsUnmountFailed -> ok
      Left err                -> unexpectedErr err
      Right _                 -> fail "Two successive unmounts succeeded"

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
  fs <- runH (newfs dev) >> mountOK dev
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
  fs <- runH (newfs dev) >> mountOK dev

  -- Check that the root directory is present and empty
  exec "openDir /" (runHalfs $ openDir fs rootPath) >>= \dh -> do
    assert =<< (== Clean) `fmap` sreadRef (dhState dh)
    assert =<< M.null     `fmap` sreadRef (dhContents dh)
    
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
    test fs p      = do
      exec ("mkdir " ++ p) (runHalfs $ mkdir fs perms p)
      isEmpty fs =<< exec ("openDir " ++ p) (runHalfs $ openDir fs p)
    -- 
    isEmpty fs dh  = do
      assert =<< null `fmap` exec ("readDir") (runHalfs $ readDir fs dh)
    -- 
    exec           = execE "propM_dirConstructionOK"

-- | Ensure that a new filesystem populated with a handful of
-- directories permits creation of a file.
propM_fileCreationOK :: HalfsProp
propM_fileCreationOK _g dev = do
  fs <- runH (newfs dev) >> mountOK dev
  
  let fooPath = rootPath </> "foo"
      fp      = fooPath </> "f1"

  exec "mkdir /foo"            $ runHalfs $ mkdir fs perms fooPath
  fh0 <- exec "create /foo/f1" $ runHalfs $ openFile fs fp True
  exec "close /foo/f1 (1)"     $ runHalfs $ closeFile fs fh0
  fh1 <- exec "reopen /foo/f1" $ runHalfs $ openFile fs fp False
  exec "close /foo/f1 (2)"     $ runHalfs $ closeFile fs fh1

  e <- runH $ openFile fs fp True
  case e of
    Left HalfsObjectExists{} -> return ()
    Left err                 -> unexpectedErr err
    Right _                  ->
      fail "Open w/ creat of existing file should fail"

  quickRemountCheck fs
  where
    exec = execE "propM_fileCreationOK"

-- | Ensure that smile write/readback works for a new file in a new
-- filesystem
propM_fileWR :: HalfsProp
propM_fileWR _g dev = do
  fs <- runH (newfs dev) >> mountOK dev
  fh <- exec "create /myfile" $
        runHalfs $ openFile fs (rootPath </> "myfile") True

  forAllM (FileWR `fmap` choose (1, maxBytes)) $ \(FileWR fileSz) -> do
  forAllM (printableBytes fileSz)              $ \fileData        -> do 

  exec "bytes -> /myfile" $ runHalfs $ write fs fh 0 fileData
  fileData' <- exec "bytes <- /myfile" $ runHalfs $ read fs fh 0 (fromIntegral fileSz)
  assert $ fileData == fileData'

  quickRemountCheck fs
  where
    exec = execE "propM_fileWR"
    maxBytes = fromIntegral $ bdBlockSize dev * bdNumBlocks dev `div` 8

-- | Sanity check: multiple threads making many new directories
-- simultaneously are interleaved in a sensible manner.
propM_dirMutexOK :: BDGeom
                 -> BlockDevice IO
                 -> PropertyM IO ()
propM_dirMutexOK _g dev = do
  fs <- runH (newfs dev) >> mountOK dev

  forAllM (DirMutexOk `fmap` choose (2, maxThreads)) $ \(DirMutexOk n) -> do
  forAllM (mapM genNm [1..n])                        $ \nmss           -> do

  ch <- run newChan
  mapM_ (run . forkIO . uncurry (threadTest fs ch)) (nmss `zip` [1..n])
  -- ^ n threads attempting to create directories simultaneously in /
  replicateM n (run $ readChan ch)
  -- ^ barrier

  dh     <- exec "openDir /" $ runHalfs $ openDir fs "/"
  dnames <- exec "readDir /" $ runHalfs $ map fst `fmap` readDir fs dh

  assert $ L.sort dnames == L.sort (concat nmss)
  quickRemountCheck fs 
  where
    maxDirs    = 100 -- } v
    maxLen     = 80  -- } arbitrary but fit into small devices
    maxThreads = 8
    exec       = execE "propM_dirMutexOK"
    ng f       = L.nub `fmap` resize maxDirs (listOf1 $ f maxLen)
    -- 
    genNm :: Int -> Gen [String]
    genNm n = map ((++) ("f" ++ show n ++ "_")) `fmap` ng filename
    -- 
    threadTest fs ch nms _n = do
      runHalfs $ mapM_ (mkdir fs perms . (</>) rootPath) nms
      writeChan ch ()
      


--------------------------------------------------------------------------------
-- Misc

ok :: Monad m => PropertyM m ()
ok = return () 
 
perms :: FileMode
perms = FileMode [Read,Write,Execute] [Read,Execute] [Read,Execute]

unexpectedErr :: (Monad m, Show a) => a -> PropertyM m ()
unexpectedErr = fail . (++) "Expected failure, but not: " . show

execE :: (Monad m, Show a) => String -> String -> m (Either a b) -> PropertyM m b
execE nm descrip f =
  run f >>= \ea -> case ea of
    Left e  ->
      fail $ "Unexpected error in " ++ nm ++ " ("
           ++ descrip ++ "): " ++ show e
    Right x -> return x

rootPath :: FilePath
rootPath = [pathSeparator]

-- Somewhat lightweight sanity check that unmounts/remounts and does a
-- hacky "equality" check on filesystem contents.  We defer to other
-- tests to more adequately test deep structural equality post-remount.
quickRemountCheck :: HalfsCapable b t r l m =>
                     HalfsState b r l m
                  -> PropertyM m ()
quickRemountCheck fs = do
  dump0 <- exec "Get dump0" $ runHalfs (dumpfs fs)
  unmountOK fs
  fs'   <- mountOK (hsBlockDev fs)
  dump1 <- exec "Get dump1" $ runHalfs (dumpfs fs')
  assert (dump0 == dump1)
  unmountOK fs'
  where
    exec = execE "quickRemountCheck"
