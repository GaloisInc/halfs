{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.CoreAPI
  (
   qcProps
  )
where

import Control.Concurrent
import qualified Data.ByteString as BS
import Data.Serialize
import Prelude hiding (read)
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic

import Halfs.BlockMap
import Halfs.Classes
import Halfs.CoreAPI hiding (sreadRef)
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
  [ -- exec 20 "Init and mount" propM_initAndMountOK
    exec 20 "Mount/unmount" propM_mountUnmountOK
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

propM_unmountMutexOK :: HalfsCapable b t r l m =>
                        BDGeom
                     -> BlockDevice m
                     -> PropertyM m ()
propM_unmountMutexOK _g dev = do
  return undefined
  where
    threadTest fs = 
      unmount fs >>= either (const $ return False) (const $ return True)

--------------------------------------------------------------------------------
-- Misc

ok :: Monad m => PropertyM m ()
ok            = return () 
 
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
      
