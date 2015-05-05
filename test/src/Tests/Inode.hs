{-# LANGUAGE Rank2Types, FlexibleContexts #-}
module Tests.Inode
  (
   qcProps
  )
where

import Control.Concurrent
import Data.ByteString (ByteString)
import Prelude hiding (read)
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
import qualified Data.ByteString   as BS
import qualified Control.Exception as CE

import Halfs.BlockMap
import Halfs.Classes
import Halfs.CoreAPI
import Halfs.Errors
import Halfs.HalfsState
import Halfs.Inode
import Halfs.Monad
import Halfs.Protection
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
  [ -- Inode module invariants
    exec 10 "Inode module invariants" propM_inodeModuleInvs
  , -- Inode stream write/read/(over)write/read property
    exec 10 "Basic WRWR" propM_basicWRWR
  , -- Inode stream write/read/(truncating)write/read property
    exec 10 "Truncating WRWR" propM_truncWRWR
  , -- Inode length-specific stream write/read
    exec 10 "Length-specific WR" propM_lengthWR
  , -- Inode single reader/writer lock testing
    exec 10 "Inode single reader/write mutex" propM_inodeMutexOK
  ]
  where
    exec = mkMemDevExec quick "Inode"


--------------------------------------------------------------------------------
-- Property Implementations

-- Note that in many of these properties we compare timestamps using <
-- and >; these may need to be <= and >= if the underlying clock
-- resolution is too coarse!

-- | Tests Inode module invariants
propM_inodeModuleInvs :: HalfsCapable b t r l m =>
                         BDGeom
                      -> BlockDevice m
                      -> PropertyM m ()
propM_inodeModuleInvs _g _dev = do
  -- Check geometry/padding invariants
  minInodeSz <- run $ computeMinimalInodeSize =<< getTime
  minExtSz  <- run $ minimalExtSize
  assert (minInodeSz == minExtSz)

-- | Tests basic write/reads & overwrites
propM_basicWRWR :: HalfsCapable b t r l m =>
                   BDGeom
                -> BlockDevice m
                -> PropertyM m ()
propM_basicWRWR _g dev = do
  withFSData dev $ \fs rdirIR dataSz testData -> do

    -- Make an inode to muck around with
    inr <- run $ blockAddrToInodeRef `fmap`
                   (alloc1 (hsBlockMap fs)
                      >>= maybe (fail "can't alloc inode") return
                   )
    run $ bdWriteBlock dev (inodeRefToBlockAddr inr)
            =<< buildEmptyInodeEnc dev RegularFile defaultFilePerms
                  inr rdirIR defaultUser defaultGroup

    let exec           = execH "propM_basicWRWR" fs
        time           = exec "obtain time" getTime
        dataSzI        = fromIntegral dataSz
        checkWriteMD t = \sz eb -> chk sz eb (t <=) (t <=) (t <=)
        checkReadMD  t = \sz eb -> chk sz eb (t <=) (t >=) (t <=)
        chk            = checkInodeMetadata fs inr RegularFile defaultFilePerms
                           defaultUser defaultGroup

  {-
    exec "Guard against interesting regression" $ do
      let dataLens = [17920, 18404, 18481]
      forM_ dataLens $ \len -> do
        let toWrite = BS.replicate len 0x58
        writeStream inr 0 True toWrite `catchError` \e ->
          trace ("e = " ++ show e) $ CE.assert False (return ())
  -}

    (_, _, api, apc) <- exec "Obtaining sizes" $ computeSizes (bdBlockSize dev)
    let expBlks = calcExpBlockCount (bdBlockSize dev) api apc dataSz

    -- Check single-byte write
    t0 <- time
    exec "Write stream 1 byte" $ writeStream inr 0 True dummyByte
    checkWriteMD t0 1 2 -- expecting 1 inode block and 1 data block

    -- Expected error: write past end of 1-byte stream (beyond block boundary)
    e0 <- runH fs $ writeStream inr (bdBlockSize dev) False testData
    case e0 of
      Left (HE_InvalidStreamIndex idx) -> assert (idx == bdBlockSize dev)
      _                                -> assert False

    -- Expected error: write past end of 1-byte stream (beyond byte boundary)
    e0' <- runH fs $ writeStream inr 2 False testData
    case e0' of
      Left (HE_InvalidStreamIndex idx) -> assert (idx == 2)
      _                                -> assert False

    -- The repeated operations may seem odd here, but their repetition and
    -- order push on a few inode data persistence bugs.

    -- Check truncation to 0 bytes
    t1 <- time
    exec "Stream trunc to 0 bytes" $ writeStream inr 0 True BS.empty
    checkWriteMD t1 0 1 -- expecting 1 inode block only (no data blocks)

    -- Check single-byte write
    t2 <- time
    exec "Write stream 1 byte" $ writeStream inr 0 True dummyByte
    checkWriteMD t2 1 2 -- expecting 1 inode block and 1 data block

    -- Check truncation to 0 bytes again
    t3 <- time
    exec "Stream trunc to 0 bytes" $ writeStream inr 0 True BS.empty
    checkWriteMD t3 0 1 -- expecting 1 inode block only (no data blocks)

    -- Non-truncating write & read-back of generated data
    t4 <- time
    exec "Non-truncating write" $ writeStream inr 0 False testData
    checkWriteMD t4 dataSzI expBlks
    t5  <- time
    bs1 <- exec "Readback 1" $ readStream inr 0 Nothing
    checkReadMD t5 dataSz expBlks

    assert (BS.length bs1 == BS.length testData)

    -- when (bs1 /= testData) $ CE.assert False $ return ()
    assert (bs1 == testData)

    -- Recheck truncation to 0 bytes
    t6 <- time
    exec "Stream trunc to 0 bytes" $ writeStream inr 0 True BS.empty
    checkWriteMD t6 0 1 -- expecting 1 inode block only (no data blocks)

    -- Non-truncating rewrite of generated data
    t7 <- time
    exec "Non-truncating write" $ writeStream inr 0 False testData
    checkWriteMD t7 dataSzI expBlks

    -- Non-truncating partial overwrite of new data & read-back
    forAllM (choose (1, dataSz `div` 2))     $ \overwriteSz -> do
      forAllM (choose (0, dataSz `div` 2 - 1)) $ \startByte   -> do
        forAllM (printableBytes overwriteSz)     $ \newData     -> do

        --   -- Useful 512 test for 512B block size
        --   let overwriteSz = 9201
        --       startByte   = 8721
        --       newData     = BS.replicate overwriteSz 0x58

          t8 <- time

          exec "Non-trunc overwrite" $
            writeStream inr (fromIntegral startByte) False newData
          checkWriteMD t8 dataSzI expBlks

          t9  <- time
          bs2 <- exec "Readback 2" $ readStream inr 0 Nothing
          checkReadMD t9 dataSz expBlks

          let expected = bsTake startByte testData
                         `BS.append`
                         newData
                         `BS.append`
                         bsDrop (startByte + overwriteSz) testData
          assert (BS.length bs2 == BS.length expected)

          when (bs2 /= expected) $ do
        --   trace ("bs2      = " ++ show bs2) $ do
        --   trace ("expected = " ++ show expected) $ do
            CE.assert False $ return ()

            assert (bs2 == expected)

            -- Check truncation to a single byte again w/ read-back
            t10 <- time
            exec "Stream trunc to 1 byte" $ writeStream inr 0 True dummyByte
            checkWriteMD t10 1 2 -- expecting 1 inode block and 1 data block
            t11  <- time
            bs3 <- exec "Readback 3" $ readStream inr 0 Nothing
            checkReadMD t11 1 2
            assert (bs3 == dummyByte)
            where
              dummyByte = BS.singleton 0

-- | Tests truncate writes and read-backs of random size
propM_truncWRWR :: HalfsCapable b t r l m =>
                   BDGeom
                -> BlockDevice m
                -> PropertyM m ()
propM_truncWRWR _g dev = do
  withFSData dev $ \fs rdirIR dataSz testData -> do
    let exec           = execH "propM_truncWRWR" fs
        time           = exec "obtain time" getTime
        numFree        = sreadRef $ bmNumFree $ hsBlockMap fs
        checkWriteMD t = \sz eb -> chk sz eb (t <=) (t <=) (t <=)
        checkReadMD  t = \sz eb -> chk sz eb (t <=) (t >=) (t <=)
        chk            = checkInodeMetadata fs rdirIR Directory rootDirPerms
                           rootUser rootGroup

    (_, _, api, apc) <- exec "Obtaining sizes" $ computeSizes (bdBlockSize dev)
    let expBlks = calcExpBlockCount (bdBlockSize dev) api apc

    -- Non-truncating write
    t1 <- time
    exec "Non-truncating write" $ writeStream rdirIR 0 False testData
    checkWriteMD t1 dataSz (expBlks dataSz)

    forAllM (choose (dataSz `div` 8, dataSz `div` 4)) $ \dataSz'   -> do
      forAllM (printableBytes dataSz')                  $ \testData' -> do
        forAllM (choose (1, dataSz - dataSz' - 1))        $ \truncIdx  -> do
          let dataSz'' = dataSz' + truncIdx
          freeBlks <- numFree -- Free blks before truncate

          -- Truncating write
          t2 <- time
          exec "Truncating write" $
            writeStream rdirIR (fromIntegral truncIdx) True testData'
          checkWriteMD t2 dataSz'' (expBlks dataSz'')

          -- Read until the end of the stream and check truncation
          t3 <- time
          bs <- exec "Readback" $ readStream rdirIR (fromIntegral truncIdx) Nothing
          checkReadMD t3 dataSz'' (expBlks dataSz'')

          assert (BS.length bs == BS.length testData')
          assert (bs == testData')

          -- Sanity check the BlockMap's free count
          freeBlks' <- numFree  -- Free blks after truncate
          let minExpectedFree = (dataSz - dataSz'') `div` (fromIntegral $ bdBlockSize dev)
          -- May also have frees on Ext storage, so this is just a lower bound

          assert $ minExpectedFree <= fromIntegral (freeBlks' - freeBlks)


-- | Tests bounded reads of random offset and length
propM_lengthWR :: HalfsCapable b t r l m =>
                  BDGeom
               -> BlockDevice m
               -> PropertyM m ()
propM_lengthWR _g dev = withFSData dev $ \fs rdirIR dataSz testData -> do
  let exec           = execH "propM_lengthWR" fs
      time           = exec "obtain time" getTime
      blkSz          = bdBlockSize dev
      checkWriteMD t = \sz eb -> chk sz eb (t <=) (t <=) (t <=)
      checkReadMD  t = \sz eb -> chk sz eb (t <=) (t >=) (t <=)
      chk            = checkInodeMetadata fs rdirIR Directory rootDirPerms
                         rootUser rootGroup

  (_, _, api, apc) <- exec "Obtaining sizes" $ computeSizes (bdBlockSize dev)
  let expBlks = calcExpBlockCount blkSz api apc dataSz

  -- Write random data to the stream
  t1 <- time
  exec "Populate" $ writeStream rdirIR 0 False testData
  checkWriteMD t1 dataSz expBlks

  -- If possible, read a minimum of one full inode + 1 byte worth of data
  -- into the next inode to push on boundary conditions & spill arithmetic.
  forAllM (arbitrary :: Gen Bool) $ \b -> do
    blksPerCarrier <- run $ if b
                             then computeNumInodeAddrsM blkSz
                             else computeNumExtAddrsM  blkSz
    let minReadLen = min dataSz (fromIntegral $ blksPerCarrier * blkSz + 1)

    forAllM (choose (minReadLen, dataSz))  $ \readLen  -> do
      forAllM (choose (0, dataSz - 1))       $ \startIdx -> do

        let readLen' = min readLen (dataSz - startIdx)
            stIdxW64 = fromIntegral startIdx

        t2 <- time
        bs <- exec "Bounded readback" $
                readStream rdirIR stIdxW64 (Just $ fromIntegral readLen')
        assert (bs == bsTake readLen' (bsDrop startIdx testData))
        checkReadMD t2 dataSz expBlks

-- | Sanity check: reads/write operations to inode streams are atomic
propM_inodeMutexOK :: BDGeom
                   -> BlockDevice IO
                   -> PropertyM IO ()
propM_inodeMutexOK _g dev = do
  fs <- runHNoEnv (newfs dev rootUser rootGroup rootDirPerms) >> mountOK dev
  rdirIR <- rootDir `fmap` sreadRef (hsSuperBlock fs)

  -- 1) Choose a random number n s.t. 8 <= n <= 32
  --
  -- 2) Create n unique bytestrings that each cover the entire randomly-sized
  --    write region.  Call this set of bytestrings 'bstrings'.
  --
  -- 3) Spawn n test threads, and create a unique bytestring for each to write.
  --    Each thread:
  --
  --      * Blindly writes its bytestring 'bs' to the stream
  --
  --      * Reads back entire stream, and ensures that what is read back
  --        is a member of bstrings; if what is read back is also not
  --        equal to what was written, we have detected interleaving.
  --
  --      * Terminates
  --
  -- We keep the write regions relatively small since we'll be creating many of
  -- them.

  forAllM (min (256*1024) `fmap` choose (lo, hi)) $ \dataSz   -> do
    forAllM (choose (8::Int, 32))                   $ \n        -> do
      forAllM (foldM (uniqueBS dataSz) [] [1..n])     $ \bstrings -> do

        ch <- run newChan

        -- ^ Kick off the test threads
        mapM_ (run . forkIO . test fs ch rdirIR dataSz bstrings) bstrings

        (passed, _interleaved) <- unzip `fmap` replicateM n (run $ readChan ch)
        -- ^ predicate barrier for all threads

        assert $ and passed

  -- The following is not needed, but it's useful information to confirm that
  -- interleaving is actually being tested (as opposed to the trivial-pass
  -- scenario where each thread reads what it just wrote).
--   when (not $ or interleaved) $
--     run $ putStrLn "WARNING: No interleaving detected in propM_inodeMutexOK"

  where
    maxBlocks = safeToInt (bdNumBlocks dev)
    lo        = blkSize * (maxBlocks `div` 32)
    hi        = blkSize * (maxBlocks `div` 16)
    blkSize   = safeToInt (bdBlockSize dev)
    --
    uniqueBS sz acc _ =
      (:acc) `fmap` (printableBytes sz `suchThat` (not . (`elem` acc)))
    --
    test fs ch inr sz bstrings bs = do
      _   <- runHalfs fs $ writeStream inr 0 False bs
      eeb <- runHalfs fs $ readStream inr 0 Nothing
      case eeb of
        Left _err -> writeChan ch (False, error "interleaved not computed")
        Right rb  -> writeChan ch (rb' `elem` bstrings, rb' /= bs)
                       where rb' = bsTake sz rb -- since unbounded readStream
                                                -- returns multiples of the
                                                -- block size

--------------------------------------------------------------------------------
-- Misc utility/helper functions

withFSData :: HalfsCapable b t r l m =>
              BlockDevice m
           -> (HalfsState b r l m -> InodeRef -> Int -> ByteString -> PropertyM m ())
           -> PropertyM m ()
withFSData dev f = do
  fs <- runHNoEnv (newfs dev rootUser rootGroup rootDirPerms) >> mountOK dev
  rdirIR <- rootDir `fmap` sreadRef (hsSuperBlock fs)
  withData dev $ f fs rdirIR

newtype FillBlocks a = FillBlocks a deriving Show
newtype SpillCnt a   = SpillCnt a deriving Show

-- Generates random data of random size between 1/8 - 1/4 of the device
withData :: HalfsCapable b t r l m =>
            BlockDevice m                          -- The blk dev
         -> (Int -> ByteString -> PropertyM m ())  -- Action
         -> PropertyM m ()
withData dev f = do
  nAddrs <- run $ computeNumExtAddrsM (bdBlockSize dev)
  let maxBlocks = safeToInt $ bdNumBlocks dev
      lo        = maxBlocks `div` 8
      hi        = maxBlocks `div` 4
      fbr       = FillBlocks `fmap` choose (lo, hi)
      scr       = SpillCnt   `fmap` choose (0, safeToInt nAddrs)
  forAllM fbr $ \(FillBlocks fillBlocks) -> do
    forAllM scr $ \(SpillCnt   spillCnt)   -> do
      -- fillBlocks is the number of blocks to fill on the write (1/8 - 1/4 of dev)
      -- spillCnt is the number of blocks to write into the last ext in the chain
      let dataSz = fillBlocks * safeToInt (bdBlockSize dev) + spillCnt
      forAllM (printableBytes dataSz) (f dataSz)
    --   -- Useful debugging test for 512B block size:
    --   let dataSz = 35 * 512 + 2 * 512
    --   f dataSz (BS.replicate dataSz 0x2E)

checkInodeMetadata :: (HalfsCapable b t r l m, Integral a) =>
                      HalfsState b r l m
                   -> InodeRef
                   -> FileType    -- expected filetype
                   -> FileMode    -- expected filemode
                   -> UserID      -- expected userid
                   -> GroupID     -- expected groupid
                   -> a           -- expected filesize
                   -> a           -- expected allocated block count
                   -> (t -> Bool) -- access time predicate
                   -> (t -> Bool) -- modification time predicate
                   -> (t -> Bool) -- status change time predicate
                   -> PropertyM m ()
checkInodeMetadata fs inr expFileTy expMode expUsr expGrp
                   expFileSz expNumBlocks accessp modifyp changep = do
  st <- execH "checkInodeMetadata" fs "filestat" $ fileStat inr
  checkFileStat st expFileSz expFileTy expMode
    expUsr expGrp expNumBlocks accessp modifyp changep
