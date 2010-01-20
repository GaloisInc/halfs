{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.Inode
  (
   qcProps
  )
where

import Control.Concurrent
import Data.ByteString (ByteString)
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

import Debug.Trace


--------------------------------------------------------------------------------
-- Inode properties

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [ -- Inode stream single write/read properties
    exec 1 "Simple WR" propM_singleWR
  ]
  where
    exec = mkMemDevExec quick "Inode"


--------------------------------------------------------------------------------
-- Property Implementations
propM_singleWR :: HalfsCapable b t r l m =>
                  BDGeom
               -> BlockDevice m
               -> PropertyM m ()
propM_singleWR _g dev = do
  fs     <- run (newfs dev) >> mountOK dev
  rdirIR <- rootDir `fmap` sreadRef (hsSuperBlock fs)

  -- Assumes fixed size BDGeom 512 512 for now
  let dsize    = 49*512+1 -- spill over slightly!
      testData = BS.replicate dsize 0x65
  ex <- run $ writeStream dev (hsBlockMap fs) rdirIR 0 False testData
  case ex of
    Left e  -> fail $ "writeStream failure in propM_singleWR: " ++ show e
    Right _ -> do
      testData' <- run $ readStream dev rdirIR 0 Nothing
      assert (testData == BS.take dsize testData')

  
