{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tests.BlockDevice
  (
   qcProps
  )
where
  
import Control.Monad.ST
import qualified Data.ByteString as BS
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
import System.Directory
import System.IO
  
import System.Device.BlockDevice
import System.Device.File
import System.Device.Memory
import System.Device.ST

import Tests.Instances

numTests :: Int -> Args
numTests n = stdArgs{maxSuccess = n}

--------------------------------------------------------------------------------
-- BlockDevice properties to check

qcProps :: [(Args, Property)]
qcProps =
  [
    ( numTests 10
    , label "File-Backed Block Device Geometry"
      $ ioBDGeomProp propM_fileBackedGeom
    )
  ,
    ( stdArgs
    , label "Memory-Backed Block Device Geometry"
      $ ioBDGeomProp propM_memBackedGeom
    )
  ,
    ( stdArgs
    , label "STArray-Backed Block Device Geometry"
      $ ioBDGeomProp propM_stArrayBackedGeom
    )
  ]

--------------------------------------------------------------------------------
-- Property implementations

-- | Checks that the geometry reported by the file-backed BlockDevice
-- corresponds to its creation parameters
propM_fileBackedGeom :: BDGeom -> PropertyM IO ()
propM_fileBackedGeom g = do
  mdev <- run $ withFileStore g $ flip newFileBlockDevice $ bdgSecSz g
  propM_geom g mdev

-- | Checks that the geometry reported by the memory-backed BlockDevice
-- corresponds to its creation parameters
propM_memBackedGeom :: BDGeom -> PropertyM IO ()
propM_memBackedGeom g = do
  mdev <- run $ newMemoryBlockDevice (bdgSecCnt g) (bdgSecSz g)
  propM_geom g mdev

-- | Checks that the geometry reported by the STArray-backed BlockDevice
-- corresponds to its creation parameters (looks like an IO property due
-- to use of stToIO)
propM_stArrayBackedGeom :: BDGeom -> PropertyM IO ()
propM_stArrayBackedGeom g = do
  -- JS: If there's a way to use propM_geom here, I couldn't figure it
  -- out.  stToIO leaves you with a BlockDevice (ST RealWorld) which
  -- isn't the same type as BlockDevice IO, even though it really is the
  -- same thing.
  let run' = run . stToIO
  mdev <- run' $ newSTBlockDevice (bdgSecCnt g) (bdgSecSz g)
  case mdev of
    Nothing  -> fail "Invalid BlockDevice"
    Just dev -> run' (bdShutdown dev) >> assert (geomConsistent g dev)

-- | Checks that the geometry reported by the given BlockDevice (if any)
-- corresponds to its creation parameters
propM_geom :: Monad m =>
              BDGeom
           -> Maybe (BlockDevice m)
           -> PropertyM m ()
propM_geom _ Nothing    = fail "Invalid BlockDevice"
propM_geom g (Just dev) = run (bdShutdown dev) >> assert (geomConsistent g dev)

geomConsistent :: Monad m => BDGeom -> BlockDevice m -> Bool
geomConsistent g dev = bdBlockSize dev == bdgSecSz g &&
                       bdNumBlocks dev == bdgSecCnt g

--------------------------------------------------------------------------------
-- Utility functions

ioBDGeomProp :: (BDGeom -> PropertyM IO ()) -> Property
ioBDGeomProp = monadicIO . forAllM arbBDGeom

withFileStore :: BDGeom -> (FilePath -> IO a) -> IO a
withFileStore geom act = do
  let numBytes = fromIntegral (bdgSecSz geom * bdgSecCnt geom)
  (fname, h) <- openTempFile "." "pseudo.dsk"
  BS.hPut h $ BS.replicate numBytes 0
  hClose h
  rslt <- act fname
  removeFile fname
  return rslt

{-
sizeMsg :: String -> BDGeom -> PropertyM IO ()
sizeMsg s g = run $ putStrLn
  $ "Checking geometry on " ++ s ++ "-backed block device ("
  ++ show (bdgSecCnt g) ++ " sectors, "
  ++ show (bdgSecSz g) ++ " bytes/sector, "
  ++ show (bdgSecSz g * bdgSecCnt g)
    ++ " bytes)"
-}
