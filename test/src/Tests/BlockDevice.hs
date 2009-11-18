{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tests.BlockDevice
  (
   qcProps
  )
where
  
import Control.Monad (forM_)
import Control.Monad.ST
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Word
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
import System.Directory
import System.IO
  
import System.Device.BlockDevice
import System.Device.File
import System.Device.Memory
import System.Device.ST

import Tests.Instances

import Debug.Trace

--------------------------------------------------------------------------------

type BDProp   = Monad m => BDGeom -> BlockDevice m -> PropertyM m ()
type PropExec = (BDGeom -> BlockDevice IO -> PropertyM IO ())
             -> BDGeom
             -> PropertyM IO ()

--------------------------------------------------------------------------------
-- BlockDevice properties to check

qcProps :: [(Args, Property)]
qcProps =
  [
  -- Geometry tests
    (numTests 10, geomProp "File"             fileBackedPropExec propM_geom)
  , (numTests 50, geomProp "Memory"           memBackedPropExec  propM_geom)
  , (numTests 50, geomProp "STArray"          staBackedPropExec  propM_geom)

  , (numTests 10, geomProp "2x Rescaled-File"
                    (rescaledExec 2 fileDev) (propM_rescaledGeom 2))
  , (numTests 50, geomProp "4x Rescaled-STArray"
                    (rescaledExec 4 staDev) (propM_rescaledGeom 4))
  , (numTests 50, geomProp "8x Rescaled-Mem"
                    (rescaledExec 8 staDev) (propM_rescaledGeom 8))

  -- Write/read tests
  , (numTests 10, wrProp "File"    id fileBackedPropExec)
  , (numTests 50, wrProp "Memory"  id memBackedPropExec)
  , (numTests 50, wrProp "STArray" id staBackedPropExec)

  , (numTests 10, wrProp "2x Rescaled-File" (scale 2) $ rescaledExec 2 fileDev)
  , (numTests 50, wrProp "4x Rescaled-ST"   (scale 4) $ rescaledExec 4 staDev)
  , (numTests 50, wrProp "8x Rescaled-Mem"  (scale 8) $ rescaledExec 8 memDev)
  ]
  where
    numTests n = stdArgs{maxSuccess = n}
    scale k g  = g{bdgScale = Just k}

    geomProp :: String -> PropExec -> BDProp -> Property
    geomProp s exec pr = labeledIOProp (s ++ "-Backed Block Device Geometry")
                         $ forAllM arbBDGeom (exec pr)

    wrProp s f exec = labeledIOProp (s ++ "-Backed Block Device Write/Read")
                      $ forAllBlocksM' f (exec . propM_writeRead)

    rescaledExec :: Word64 -> DevCtor -> PropExec
    rescaledExec k d f g = f g `usingBD` rescaledDev g k d

--------------------------------------------------------------------------------
-- Property implementations

-- | Checks that the geometry reported by the given BlockDevice (if any)
-- corresponds to its creation parameters
propM_geom :: Monad m => BDGeom -> BlockDevice m -> PropertyM m ()
propM_geom g dev =
  assert $ bdBlockSize dev == bdgSecSz g &&
           bdNumBlocks dev == bdgSecCnt g   

-- | Checks that the geometry reported by the given (rescaled) BlockDevice
-- corresponds correctly to the creation parameters of the underlying device and
-- the linear scale factor used to rescale it
propM_rescaledGeom :: Monad m =>
                      Word64
                   -> BDGeom
                   -> BlockDevice m
                   -> PropertyM m ()
propM_rescaledGeom k origGeom dev =
  propM_geom newGeom dev
  where
    newGeom = BDGeom (bdgSecCnt origGeom `div` k)
                     (bdgSecSz origGeom * k)
                     (Just k)

-- | Checks that blocks written to the Block Device can be read back
-- immediately after each is written.
propM_writeRead :: Monad m =>
                   [(Word64, ByteString)]
                -> BDGeom
                -> BlockDevice m
                -> PropertyM m ()
propM_writeRead fsData _g dev = do
    forM_ fsData $ \(blkAddr, blkData) -> do
      run $ bdWriteBlock dev blkAddr blkData
      run $ bdFlush dev
      bs <- run $ bdReadBlock dev blkAddr
      assert $ blkData == bs

--------------------------------------------------------------------------------
-- Utility functions

type DevCtor = BDGeom -> IO (Maybe (BlockDevice IO))
           
-- | Runs the given property on the file-backed block device
fileBackedPropExec :: PropExec
fileBackedPropExec f g = f g `usingBD` fileDev g

-- | Runs the given property on a memory-backed block device
memBackedPropExec :: PropExec
memBackedPropExec f g = f g `usingBD` memDev g

-- | Runs the given property on an STArray-backed block device
staBackedPropExec :: PropExec
staBackedPropExec f g = f g `usingBD` staDev g

fileDev :: DevCtor
fileDev g = withFileStore g (`newFileBlockDevice` (bdgSecSz g))

memDev :: DevCtor
memDev g = newMemoryBlockDevice (bdgSecCnt g) (bdgSecSz g)

-- | Create an STArray-backed block device.  This function transforms
-- the underlying ST-based block device an IO block device for interface
-- consistency within this module.
staDev :: DevCtor
staDev g = stBDToIOBD $ newSTBlockDevice (bdgSecCnt g) (bdgSecSz g)

rescaledDev :: BDGeom  -- ^ geometry for underlying device
            -> Word64  -- ^ block size scale factor
            -> DevCtor -- ^ ctor for underlying device
            -> IO (Maybe (BlockDevice IO))
rescaledDev g k ctor = 
  maybe (fail "Invalid BlockDevice") (newRescaledBlockDevice (k * bdgSecSz g))
    `fmap` ctor g

stBDToIOBD :: ST RealWorld (Maybe (BlockDevice (ST RealWorld)))
           -> IO (Maybe (BlockDevice IO))
stBDToIOBD stbd = do
  mdev <- stToIO stbd
  case mdev of
    Nothing     -> return Nothing
    Just oldDev -> return $! Just BlockDevice {
        bdBlockSize  = bdBlockSize oldDev
      , bdNumBlocks  = bdNumBlocks oldDev
      , bdReadBlock  = \i -> stToIO $ bdReadBlock oldDev i
      , bdWriteBlock = \i v -> stToIO $ bdWriteBlock oldDev i v
      , bdFlush      = stToIO $ bdFlush oldDev
      , bdShutdown   = stToIO $ bdShutdown oldDev
      }

usingBD :: Monad m =>
          (BlockDevice m -> PropertyM m ())
       -> m (Maybe (BlockDevice m))
       -> PropertyM m ()
usingBD f act = do
  mdev <- run act
  case mdev of
    Nothing  -> fail "Invalid BlockDevice"
    Just dev -> f dev >> run (bdShutdown dev)

labeledIOProp :: String -> PropertyM IO a -> Property
labeledIOProp s p = label s (monadicIO p)

withFileStore :: BDGeom -> (FilePath -> IO a) -> IO a
withFileStore geom act = do
  let numBytes = fromIntegral (bdgSecSz geom * bdgSecCnt geom)
  (fname, h) <- openTempFile "." "pseudo.dsk"
  BS.hPut h $ BS.replicate numBytes 0
  hClose h
  rslt <- act fname
  removeFile fname
  return rslt
