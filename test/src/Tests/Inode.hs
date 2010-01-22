{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.Inode
  (
   qcProps
  )
where

import Control.Concurrent
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.Map as M
import Data.Time
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
import Tests.Instances (printableBytes)
import Tests.Types
import Tests.Utils

import Debug.Trace
import System.Exit
import System.IO.Unsafe (unsafePerformIO)

--------------------------------------------------------------------------------
-- Inode properties

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [ -- Inode stream single write/read properties
    exec 1 "Simple WR" propM_basicWR
  ]
  where
    exec = mkMemDevExec quick "Inode"


--------------------------------------------------------------------------------
-- Property Implementations
propM_basicWR :: HalfsCapable b t r l m =>
                  BDGeom
               -> BlockDevice m
               -> PropertyM m ()
propM_basicWR _g dev = do
  fs        <- run (newfs dev) >> mountOK dev
  rdirIR    <- rootDir `fmap` sreadRef (hsSuperBlock fs)
  nAddrs    <- run $ computeNumAddrsM (bdBlockSize dev)
  let maxBlocks = safeToInt $ bdNumBlocks dev
      bm        = hsBlockMap fs 

  forAllM (choose (maxBlocks `div` 8, maxBlocks `div` 4)) $ \fillBlocks -> do
  forAllM (choose (0, safeToInt nAddrs))                  $ \spillCnt   -> do
  -- fillBlocks is the number of blocks to fill on the write (1/8 - 1/4 of dev)
  -- spillCnt is the number of blocks to write into the last inode in the chain
  let dataSz = fillBlocks * safeToInt (bdBlockSize dev) + spillCnt
  forAllM (printableBytes dataSz) $ \testData -> do
                          
  trace ("\n=============================================================") $ do
  trace ("propM_basicWR: nAddrs                 = " ++ show nAddrs)               $ do
  trace ("propM_basicWR: fillBlocks             = " ++ show fillBlocks)           $ do
  trace ("propM_basicWR: spillCnt               = " ++ show spillCnt)             $ do
  trace ("propM_basicWR: dataSz                 = " ++ show dataSz)               $ do
  trace ("propM_basicWR: length testData        = " ++ show (BS.length testData)) $ do

  -- Non-truncating write & read-back
  e1 <- run $ writeStream dev bm rdirIR 0 False testData
  case e1 of
    Left  e -> fail $ "writeStream failure in propM_basicWR: " ++ show e
    Right _ -> do
      testData' <- bsTake dataSz `fmap` (run $ readStream dev rdirIR 0 Nothing)
      -- ^ We leave off the trailing bytes of what we read, since reading until
      -- the end of the stream will include contents of the whole last block
      assert (testData == testData')

  trace ("propM_basicWR: Non-truncating overwrite & read-back: ") $ do

  -- Non-truncating overwrite & read-back
  let overwriteSz = 513
      startByte   = 0

-- HERE 21 Jan 2010: Change startByte to 1 and investigate! t2 is failing below,
-- but furthermore the lengths differ ... also double-check logic of t1, t2, t3
-- below for non-zero start bytes =/

--  forAllM (choose (1, dataSz `div` 2))     $ \overwriteSz -> do 
--  forAllM (choose (0, dataSz `div` 2 - 1)) $ \startByte   -> do
  forAllM (printableBytes overwriteSz)     $ \newData     -> do

  trace ("propM_basicWR: overwriteSz = " ++ show overwriteSz) $ do
  trace ("propM_basicWR: startByte   = " ++ show startByte) $ do
  -- trace ("propM_basicWR: newData     = " ++ show newData) $ do

  e2 <- run $ writeStream dev bm rdirIR (fromIntegral startByte) False newData

  case e2 of
    Left  e -> fail $ "writeStream failure in propM_basicWR: " ++ show e
    Right _ -> do
      readBack <- bsTake dataSz `fmap` (run $ readStream dev rdirIR 0 Nothing)
      let expected = bsTake (startByte - 1) testData
                     `BS.append`
                     newData
                     `BS.append`
                     bsDrop (startByte + overwriteSz) testData
      trace ("BS.length expected = " ++ show (BS.length expected)) $ do
      trace ("BS.length readBack = " ++ show (BS.length readBack)) $ do

      let t1 = bsTake (startByte - 1) testData           == bsTake (startByte - 1) readBack
          t2 = newData                                   == bsTake overwriteSz (bsDrop (startByte - 1) readBack)
          t3 = bsDrop (startByte + overwriteSz) testData == bsDrop (startByte + overwriteSz) readBack

      trace ("Case t1: " ++ show t1) $ do
      trace ("Case t2: " ++ show t2) $ do
      trace ("Case t3: " ++ show t3) $ do

      when (readBack /= expected) $
        trace ("readBack /= expected, aborting") $ unsafePerformIO exitFailure
      assert (readBack == expected)
                         
  return ()  

{-
  -- Assumes fixed size BDGeom 512 512 !
  let dsize    = 49*512+1 -- spill over slightly!
      testData = BS.replicate dsize 0x65
  ex <- run $ writeStream dev (hsBlockMap fs) rdirIR 0 False testData
  case ex of
    Left e  -> fail $ "writeStream failure in propM_basicWR: " ++ show e
    Right _ -> do
      testData' <- run $ readStream dev rdirIR 0 Nothing
      assert (testData == BS.take dsize testData')
-}



             
  
