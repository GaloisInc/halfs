{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.Inode
  (
   qcProps
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Prelude hiding (read)
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic

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
import Tests.Instances           (printableBytes)
import Tests.Types
import Tests.Utils

-- import Debug.Trace


--------------------------------------------------------------------------------
-- Inode properties

qcProps :: Bool -> [(Args, Property)]
qcProps quick =
  [ -- Inode stream write/read/(over)write/read property
    exec 10 "Simple WRWR" propM_basicWRWR
  ,
    -- Inode stream write/read/(truncating)write/read property
    exec 10 "Truncating WRWR" propM_truncWRWR
  ,
    -- Inode length-specific stream write/read
    exec 10 "Length-specific WR" propM_lengthWR
  ]
  where
    exec = mkMemDevExec quick "Inode"


--------------------------------------------------------------------------------
-- Property Implementations

-- | Tests basic write/reads & overwrites
propM_basicWRWR :: HalfsCapable b t r l m =>
                   BDGeom
                -> BlockDevice m
                -> PropertyM m ()
propM_basicWRWR _g dev = do
  withFSData dev $ \bm rdirIR dataSz testData -> do 

  -- Expected error: attempted write past end of (empty) stream
  e0 <- runH $ writeStream dev bm rdirIR (bdBlockSize dev) False testData
  case e0 of
    Left (HalfsInvalidStreamIndex idx) -> assert (idx == bdBlockSize dev)
    _                                  -> assert False
                                        
  -- TODO: Fix this: need to catch byte offset errors, not just
  --       block/cont offset errors
{-
  e0' <- runH $ writeStream dev bm rdirIR 5 False testData
  case e0' of
    Left (HalfsInvalidStreamIndex idx) -> assert (idx == 1)
   _                                  -> assert False
-}

  -- Non-truncating write & read-back
  e1 <- runH $ writeStream dev bm rdirIR 0 False testData
  case e1 of
    Left  e -> fail $ "writeStream failure in propM_basicWRWR: " ++ show e
    Right _ -> do
      ebs <- runH $ readStream dev rdirIR 0 Nothing
      case ebs of
        Left e -> fail $ "readStream failure in propM_basicWRWR: " ++ show e
        Right bs ->
          -- ^ We leave off the trailing bytes of what we read, since reading
          -- until the end of the stream will include contents of the whole last
          -- block
          assert (testData == bsTake dataSz bs)

  -- Non-truncating overwrite & read-back
  forAllM (choose (1, dataSz `div` 2))     $ \overwriteSz -> do 
  forAllM (choose (0, dataSz `div` 2 - 1)) $ \startByte   -> do
  forAllM (printableBytes overwriteSz)     $ \newData     -> do
  e2 <- runH $ writeStream dev bm rdirIR (fromIntegral startByte) False newData
  case e2 of
    Left  e -> fail $ "writeStream failure in propM_basicWRWR: " ++ show e
    Right _ -> do
      ebs <- runH $ readStream dev rdirIR 0 Nothing
      case ebs of
        Left e   -> fail $ "readStream failure in propM_basicWRWR: " ++ show e
        Right bs -> do 
          let readBack = bsTake dataSz bs
              expected = bsTake startByte testData
                         `BS.append`
                         newData
                         `BS.append`
                         bsDrop (startByte + overwriteSz) testData
          assert (readBack == expected)

-- | Tests truncate writes and read-backs of random size
propM_truncWRWR :: HalfsCapable b t r l m =>
                   BDGeom
                -> BlockDevice m
                -> PropertyM m ()
propM_truncWRWR _g dev = do
  withFSData dev $ \bm rdirIR dataSz testData -> do 
  -- Non-truncating write
  e1 <- runH $ writeStream dev bm rdirIR 0 False testData
  case e1 of
    Left e  -> fail $ "writeStream failure in propM_truncWRWR: " ++ show e
    Right _ -> do
      forAllM (choose (dataSz `div` 8, dataSz `div` 4)) $ \dataSz'   -> do
      forAllM (printableBytes dataSz')                  $ \testData' -> do 

      freeBlks <- sreadRef (bmNumFree bm) -- Free blks before truncate

      -- Truncating write
      e2 <- runH $ writeStream dev bm rdirIR 1 True testData'
      case e2 of
        Left e  -> fail $ "writeStream failure in propM_truncWRWR: " ++ show e
        Right _ -> do 
          -- Read until the end of the stream and check truncation       
          ebs <- runH $ readStream dev rdirIR 1 Nothing
          case ebs of
            Left e   -> fail $ "readStream failure in propM_truncWRWR: " ++ show e
            Right bs -> do
              assert (bsTake dataSz' bs == testData')
              assert (all (== truncSentinel) $ BS.unpack $ bsDrop dataSz' bs)
              -- Sanity check the BlockMap' free count
              freeBlks' <- sreadRef (bmNumFree bm)
              let minExpectedFree = -- also may have frees on inode storage, so
                                    -- this is just a lower bound sanity check
                    (dataSz - dataSz') `div` (fromIntegral $ bdBlockSize dev)
              assert (minExpectedFree <= fromIntegral (freeBlks' - freeBlks))
                 
-- | Tests bounded reads of random offset and length
propM_lengthWR :: HalfsCapable b t r l m =>
                  BDGeom
               -> BlockDevice m
               -> PropertyM m ()
propM_lengthWR _g dev = do
  withFSData dev $ \bm rdirIR dataSz testData -> do 
  let blkSz = bdBlockSize dev                        
  e1 <- runH $ writeStream dev bm rdirIR 0 False testData
  case e1 of
    Left e  -> fail $ "writeStream failure in propM_lengthWR: " ++ show e
    Right _ -> do
      -- If possible, read a minimum of one full inode + 1 byte worth of data
      -- into the next inode to push on boundary conditions & spill arithmetic.

      forAllM (arbitrary :: Gen Bool) $ \b -> do
      blksPerCarrier <- run $
        if b then computeNumInodeAddrsM blkSz else computeNumContAddrsM  blkSz
      let minReadLen = min dataSz (fromIntegral $ blksPerCarrier * blkSz + 1)

      forAllM (choose (minReadLen, dataSz))  $ \readLen  -> do
      forAllM (choose (0, dataSz - 1))       $ \startIdx -> do

      let readLen' = min readLen (dataSz - startIdx)
          stIdxW64 = fromIntegral startIdx

      ebs <- runH $ readStream dev rdirIR stIdxW64 (Just $ fromIntegral readLen')
      case ebs of
        Left e   -> fail $ "readStream failure in propM_lengthWR: " ++ show e
        Right bs -> 
          assert (bs == bsTake readLen' (bsDrop startIdx testData))

withFSData :: HalfsCapable b t r l m =>
              BlockDevice m
           -> (BlockMap b r l -> InodeRef -> Int -> ByteString -> PropertyM m ())
           -> PropertyM m ()
withFSData dev f = do
  fs <- runH (newfs dev) >> mountOK dev
  let bm = hsBlockMap fs 
  rdirIR <- rootDir `fmap` sreadRef (hsSuperBlock fs)
  withData dev $ f bm rdirIR 

newtype FillBlocks a = FillBlocks a deriving Show
newtype SpillCnt a   = SpillCnt a deriving Show

-- Generates random data of random size between 1/8 - 1/4 of the device
withData :: HalfsCapable b t r l m =>
            BlockDevice m                          -- The blk dev
         -> (Int -> ByteString -> PropertyM m ())  -- Action
         -> PropertyM m ()
withData dev f = do
  nAddrs <- run $ computeNumContAddrsM (bdBlockSize dev)
  let maxBlocks = safeToInt $ bdNumBlocks dev
      lo        = maxBlocks `div` 8
      hi        = maxBlocks `div` 4
      fbr       = FillBlocks `fmap` choose (lo, hi)
      scr       = SpillCnt   `fmap` choose (0, safeToInt nAddrs)
  forAllM fbr $ \(FillBlocks fillBlocks) -> do
  forAllM scr $ \(SpillCnt   spillCnt)   -> do
  -- fillBlocks is the number of blocks to fill on the write (1/8 - 1/4 of dev)
  -- spillCnt is the number of blocks to write into the last cont in the chain
  let dataSz = fillBlocks * safeToInt (bdBlockSize dev) + spillCnt
--  let dataSz = 48 * 512 + 56 * 512 + 1
  forAllM (printableBytes dataSz) (f dataSz)
          
