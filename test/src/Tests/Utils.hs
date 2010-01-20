{-# LANGUAGE Rank2Types, FlexibleContexts #-}

module Tests.Utils
where

import Control.Monad.ST
import qualified Data.ByteString as BS
import System.IO
import System.IO.Unsafe (unsafePerformIO)
import System.Directory
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic

import Halfs.Classes
import Halfs.CoreAPI (mount)
import Halfs.Monad
  
import System.Device.BlockDevice
import System.Device.File
import System.Device.Memory
import System.Device.ST

import Tests.Instances
import Tests.Types

type DevCtor = BDGeom -> IO (Maybe (BlockDevice IO))

--------------------------------------------------------------------------------
-- Utility functions

fileDev :: DevCtor
fileDev g = withFileStore g (`newFileBlockDevice` (bdgSecSz g))

memDev :: DevCtor
memDev g = newMemoryBlockDevice (bdgSecCnt g) (bdgSecSz g)

-- | Create an STArray-backed block device.  This function transforms
-- the ST-based block device to an IO block device for interface
-- consistency within this module.
staDev :: DevCtor
staDev g =
  stToIO (newSTBlockDevice (bdgSecCnt g) (bdgSecSz g)) >>= 
  return . maybe Nothing (\dev ->
    Just BlockDevice {
        bdBlockSize = bdBlockSize dev
      , bdNumBlocks = bdNumBlocks dev
      , bdReadBlock  = \i   -> stToIO $ bdReadBlock dev i
      , bdWriteBlock = \i v -> stToIO $ bdWriteBlock dev i v
      , bdFlush      = stToIO $ bdFlush dev
      , bdShutdown   = stToIO $ bdShutdown dev
    })

rescaledDev :: BDGeom  -- ^ geometry for underlying device
            -> BDGeom  -- ^ new device geometry 
            -> DevCtor -- ^ ctor for underlying device
            -> IO (Maybe (BlockDevice IO))
rescaledDev oldG newG ctor = 
  maybe (fail "Invalid BlockDevice") (newRescaledBlockDevice (bdgSecSz newG))
    `fmap` ctor oldG

monadicBCMIOProp :: PropertyM (BCM IO) a -> Property
monadicBCMIOProp = monadic (unsafePerformIO . runBCM)

withFileStore :: BDGeom -> (FilePath -> IO a) -> IO a
withFileStore geom act = do
  let numBytes = fromIntegral (bdgSecSz geom * bdgSecCnt geom)
  (fname, h) <- openTempFile "." "pseudo.dsk"
  BS.hPut h $ BS.replicate numBytes 0
  hClose h
  rslt <- act fname
  removeFile fname
  return rslt

whenDev :: (Monad m) => (a -> m b) -> (a -> m ()) -> Maybe a -> m b
whenDev act cleanup =
  maybe (fail "Invalid BlockDevice") $ \x -> do
    y <- act x
    cleanup x
    return y

mkMemDevExec :: forall m.
                Bool
             -> String
             -> Int
             -> String
             -> (BDGeom -> BlockDevice IO -> PropertyM IO m)
             -> (Args, Property)
mkMemDevExec quick pfx =
  let numTests n = (,) $ if quick then stdArgs{maxSuccess = n} else stdArgs
      doProp     = (`whenDev` run . bdShutdown)
  in
    \n s pr ->
      numTests n $ label (pfx ++ ": " ++ s) $ monadicIO $
        forAllM arbBDGeom $ \g ->
          run (memDev g) >>= doProp (pr g)

mountOK :: HalfsCapable b t r l m =>
           BlockDevice m
        -> PropertyM m (Halfs b r m l)
mountOK dev =
  run (mount dev)
  >>= either (fail . (++) "Unexpected mount failure: " . show) (return)

sreadRef :: HalfsCapable b t r l m => r a -> PropertyM m a
sreadRef = ($!) (run . readRef)
