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
  [ -- Inode stream write/read/(over)write/read property
--    exec 1 "Simple WRWR" propM_basicWRWR
    -- Inode stream write/read/(truncating)write/read property
--  ,
  exec 100 "Truncating WRWR" propM_truncWRWR
  ]
  where
    exec = mkMemDevExec quick "Inode"


--------------------------------------------------------------------------------
-- Property Implementations

propM_truncWRWR :: HalfsCapable b t r l m =>
                   BDGeom
                -> BlockDevice m
                -> PropertyM m ()
propM_truncWRWR g dev = do
  trace ("propM_truncWRWR: geom = " ++ show g) $ do
  fs <- run (newfs dev) >> mountOK dev
  let bm = hsBlockMap fs 
  rdirIR <- rootDir `fmap` sreadRef (hsSuperBlock fs)
  withData dev $ \dataSz testData -> do
  trace ("dataSz = " ++ show dataSz) $ do
  
  -- Non-truncating write
  e1 <- run $ writeStream dev bm rdirIR 0 False testData
  case e1 of
    Left e  -> fail $ "writeStream failure in propM_truncWRWR: " ++ show e
    Right _ -> do
      -- Truncating write
--     let dataSz'   = 10239
      forAllM (choose (dataSz `div` 8, dataSz `div` 4)) $ \dataSz'   -> do
      forAllM (printableBytes dataSz')                  $ \testData' -> do 

--      let dataSz'   = 8
--          testData' = BS.replicate dataSz' 0
      trace ("dataSz' = " ++ show dataSz') $ do

      e2 <- run $ writeStream dev bm rdirIR 1 True testData'
      case e2 of
        Left e  -> fail $ "writeStream failure in propM_truncWRWR: " ++ show e
        Right _ -> do 
          readBack <- run $ readStream dev rdirIR 1 Nothing
          trace ("BS.length readBack = " ++ show (BS.length readBack)) $ do
--          trace ("readBack = " ++ show readBack) $ do

--          trace ("p1: " ++ show (bsTake dataSz' readBack == testData')) $ do
          assert (bsTake dataSz' readBack == testData')

--          trace ("p2: " ++ show (all (== truncSentinel) $ BS.unpack $ bsDrop dataSz' readBack)) $ do
--          trace ("p2_detail: readBack remainder length = " ++ show (BS.length $ bsDrop dataSz' readBack)) $ do
--          trace ("p2_detail: readBack remainder = " ++ show (bsDrop dataSz' readBack)) $ do
          assert (all (== truncSentinel) $ BS.unpack $ bsDrop dataSz' readBack)

propM_basicWRWR :: HalfsCapable b t r l m =>
                   BDGeom
                -> BlockDevice m
                -> PropertyM m ()
propM_basicWRWR _g dev = do
  fs <- run (newfs dev) >> mountOK dev
  let bm = hsBlockMap fs 
  rdirIR <- rootDir `fmap` sreadRef (hsSuperBlock fs)
  withData dev $ \dataSz testData -> do

  -- Non-truncating write & read-back
  e1 <- run $ writeStream dev bm rdirIR 0 False testData
  case e1 of
    Left  e -> fail $ "writeStream failure in propM_basicWR: " ++ show e
    Right _ -> do
      testData' <- bsTake dataSz `fmap` (run $ readStream dev rdirIR 0 Nothing)
      -- ^ We leave off the trailing bytes of what we read, since reading until
      -- the end of the stream will include contents of the whole last block
      assert (testData == testData')

--  trace ("propM_basicWR: Non-truncating overwrite & read-back: ") $ do

  -- Non-truncating overwrite & read-back
  forAllM (choose (1, dataSz `div` 2))     $ \overwriteSz -> do 
  forAllM (choose (0, dataSz `div` 2 - 1)) $ \startByte   -> do
  forAllM (printableBytes overwriteSz)     $ \newData     -> do
  e2 <- run $ writeStream dev bm rdirIR (fromIntegral startByte) False newData
  case e2 of
    Left  e -> fail $ "writeStream failure in propM_basicWR: " ++ show e
    Right _ -> do
      readBack <- bsTake dataSz `fmap` (run $ readStream dev rdirIR 0 Nothing)
      let expected = bsTake startByte testData
                     `BS.append`
                     newData
                     `BS.append`
                     bsDrop (startByte + overwriteSz) testData
      assert (readBack == expected)

-- Generates random data of random size between 1/8 - 1/4 of the device
withData :: HalfsCapable b t r l m =>
            BlockDevice m                          -- The blk dev
         -> (Int -> ByteString -> PropertyM m ())  -- Action
         -> PropertyM m ()
withData dev f = do
  nAddrs <- run $ computeNumAddrsM (bdBlockSize dev)
  let maxBlocks = safeToInt $ bdNumBlocks dev
  forAllM (choose (maxBlocks `div` 8, maxBlocks `div` 4)) $ \fillBlocks -> do
  forAllM (choose (0, safeToInt nAddrs))                  $ \spillCnt   -> do
  -- fillBlocks is the number of blocks to fill on the write (1/8 - 1/4 of dev)
  -- spillCnt is the number of blocks to write into the last inode in the chain
  let dataSz = fillBlocks * safeToInt (bdBlockSize dev) + spillCnt
--  let dataSz = safeToInt $ nAddrs * (bdBlockSize dev) + 1
--  let dataSz = 103 * safeToInt (bdBlockSize dev) + 21
  forAllM (printableBytes dataSz) (f dataSz)
