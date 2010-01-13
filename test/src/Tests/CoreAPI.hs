{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.CoreAPI
  (
   qcProps
  )
where

import Control.Concurrent
import qualified Data.ByteString as BS
import qualified Data.Map as M
import Data.Serialize
import Prelude hiding (read)
import System.FilePath
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic

import Halfs.BlockMap
import Halfs.Classes
import Halfs.CoreAPI
import Halfs.Directory
import Halfs.Errors
import Halfs.Inode
import Halfs.Monad
import Halfs.SuperBlock

import System.Device.BlockDevice (BlockDevice(..))
import Tests.Instances ()
import Tests.Types
import Tests.Utils

-- import Debug.Trace


--------------------------------------------------------------------------------
-- CoreAPI properties

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [
--     exec 50  "Init and mount" propM_initAndMountOK
--   ,
--     exec 50  "Mount/unmount"  propM_mountUnmountOK
--   ,
--     exec 50  "Unmount mutex"  propM_unmountMutexOK
--   ,
     exec 1 "Directory construction" propM_dirConstructionOK
  ]
  where
    exec = mkMemDevExec quick "CoreAPI"


--------------------------------------------------------------------------------
-- Property Implementations

propM_initAndMountOK :: HalfsCapable b t r l m =>
                        BDGeom
                     -> BlockDevice m
                     -> PropertyM m ()
propM_initAndMountOK _g dev = do
  sb <- run (newfs dev)
  assert $ 1 == version sb
  assert $ unmountClean sb
  assert $ nilInodeRef /= rootDir sb

  -- Mount the filesystem & ensure integrity with newfs contents
  run (mount dev) >>= \efs -> case efs of 
    Left  err -> fail $ "Mount failed: " ++ show err
    Right fs  -> do
      sb'      <- sreadRef (hsSuperBlock fs)
      freeBlks <- sreadRef (bmNumFree $ hsBlockMap fs)
      assert $ not $ unmountClean sb'
      assert $ sb' == sb{ unmountClean = False }
      assert $ freeBlocks sb' == freeBlks

  -- Mount the filesystem again and ensure "dirty unmount" failure
  run (mount dev) >>= \efs -> case efs of
    Left (HalfsMountFailed DirtyUnmount) -> ok
    Left err                             -> unexpectedErr err
    Right _                              ->
      fail "Mount of unclean filesystem succeeded"

  -- Corrupt the superblock and ensure "bad superblock" failure
  run $ bdWriteBlock dev 0 $ BS.replicate (fromIntegral $ bdBlockSize dev) 0
  run (mount dev) >>= \efs -> case efs of
    Left (HalfsMountFailed BadSuperBlock{}) -> ok
    Left err                                -> unexpectedErr err
    Right _                                 ->
      fail "Mount of filesystem with corrupt superblock succeeded"
  where

propM_mountUnmountOK :: HalfsCapable b t r l m =>
                        BDGeom
                     -> BlockDevice m
                     -> PropertyM m ()
propM_mountUnmountOK _g dev = do
  run (newfs dev)
  fs <- mountOK dev
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
    unmountOK fs'                             -- Unmount #1
    run (unmount fs') >>= \efs -> case efs of -- Unmount #2
      Left HalfsUnmountFailed -> ok
      Left err                -> unexpectedErr err
      Right _                 -> fail "Two successive unmounts succeeded"
  where
    unmountOK fs =
      run (unmount fs)
      >>= either (fail . (++) "Unxpected unmount failure: " . show) (const ok)

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
  fs <- run (newfs dev) >> mountOK dev
  ch <- run newChan
  run $ forkIO $ threadTest fs ch
  run $ threadTest fs ch
  r1 <- run $ readChan ch
  r2 <- run $ readChan ch
  assert $ (r1 || r2) && not (r1 && r2) -- r1 `xor` r2
  where
    threadTest fs ch =
      unmount fs >>= either (\_ -> writeChan ch False) (\_ -> writeChan ch True)

-- | Ensure that a new filesystem has the expected root directory intact
-- and that various directory operations work as expected.
propM_dirConstructionOK :: HalfsCapable b t r l m =>
                        BDGeom
                     -> BlockDevice m
                     -> PropertyM m ()
propM_dirConstructionOK _g dev = do
  fs <- run (newfs dev) >> mountOK dev

  -- Check that the root directory is present but empty
  exec "openDir on root directory" (openDir fs rootPath) >>= \dh -> do
    dirState <- sreadRef $ dhState dh
    contents <- sreadRef $ dhContents dh
    assert $ dirState == Clean
    assert $ M.null contents
    
  -- TEMP/XXX: beg hacky tests for fixed device geometry BDGeom 512 512
  rdirIR <- rootDir `fmap` sreadRef (hsSuperBlock fs)
  exec "tmp test" $ writeStream dev (hsBlockMap fs) rdirIR 0 True (BS.replicate (49*512+1) 0xFF)
  -- TEMP/XXX: end hacky tests for fixed device geometry BDGeom 512 512

  -- TODO: re-enable!
  -- exec "mkdir in root directory" $ mkdir fs (rootPath </> "foo") perms

  where
    rootPath       = [pathSeparator]
    perms          = FileMode [Read,Write,Execute] [Read,Execute] [Read,Execute]
    exec descrip f = run f >>= \ea -> case ea of
                     Left e  ->
                       fail $ "Unexpected error in propM_dirConstructionOK ("
                              ++ descrip ++ "):" ++ show e
                     Right x -> return x


--------------------------------------------------------------------------------
-- Misc

ok :: Monad m => PropertyM m ()
ok = return () 
 
unexpectedErr :: (Monad m, Show a) => a -> PropertyM m ()
unexpectedErr = fail . (++) "Expected failure, but not: " . show

sreadRef :: HalfsCapable b t r l m => r a -> PropertyM m a
sreadRef = ($!) (run . readRef)

mountOK :: HalfsCapable b t r l m =>
           BlockDevice m
        -> PropertyM m (Halfs b r m l)
mountOK dev =
  run (mount dev)
  >>= either (fail . (++) "Unexpected mount failure: " . show) (return)
