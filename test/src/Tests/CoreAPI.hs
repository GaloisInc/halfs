{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.CoreAPI
  (
   qcProps
  )
where

import qualified Data.ByteString as BS
import Prelude hiding (read)
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic

import Halfs.BlockMap
import Halfs.Classes
import Halfs.CoreAPI
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
  [ exec 20 "Init and mount" propM_initAndMountOK
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
  sb <- run $ newfs dev
  assert $ 1 == version sb
  assert $ unmountClean sb
  assert $ nilInodeRef /= rootDir sb

  -- Mount the filesystem & ensure integrity with newfs contents
  run (mount dev) >>= \efs -> case efs of 
    Left  err -> fail $ "Mount failed: " ++ show err
    Right fs  -> do
      sb'      <- dref $ hsSuperBlock fs
      freeBlks <- dref $ bmNumFree $ hsBlockMap fs
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
    ok            = return () 
    unexpectedErr = fail . (++) "Expected failure, but not: " . show
    dref          = ($!) (run . readRef)

