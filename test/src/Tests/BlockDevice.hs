module Tests.BlockDevice
  (
   qcProps
  )
where
  
import qualified Data.ByteString as BS
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic
import System.Directory
import System.IO
  
import System.Device.BlockDevice
import System.Device.File
import System.Device.Memory
-- import System.Device.ST

import Tests.Instances

numTests :: Int -> Args
numTests n = stdArgs{maxSuccess = n}

qcProps :: [(Args, Property)]
qcProps =
  [
    ( numTests 10
    , label "File-Backed Block Device Geometry"
      $ ioBDGeomProp propM_fileBackedSize
    )
  ,
    ( numTests 50
    , label "Memory-Backed Block Device Geometry"
      $ ioBDGeomProp propM_memBackedSize
    )
  ]

-- | Checks that the size reported by the file-backed BlockDevice
-- corresponds to its creation parameters
propM_fileBackedSize :: BDGeom -> PropertyM IO ()
propM_fileBackedSize g = do
--  sizeMsg "file" g
  propM_size g
    =<< (run $ withFileStore g $ flip newFileBlockDevice $ bdgSecSz g)

-- | Checks that the size reported by the memory-backed BlockDevice
-- corresponds to its creation parameters
propM_memBackedSize :: BDGeom -> PropertyM IO ()
propM_memBackedSize g = do
--  sizeMsg "mem" g
  propM_size g =<<
    (run $ newMemoryBlockDevice (bdgSecCnt g) (bdgSecSz g))

withFileStore :: BDGeom -> (FilePath -> IO a) -> IO a
withFileStore geom act = do
  let numBytes = fromIntegral (bdgSecSz geom * bdgSecCnt geom)
  (fname, h) <- openTempFile "." "pseudo.dsk"
  BS.hPut h $ BS.replicate numBytes 0
  hClose h
  rslt <- act fname
  removeFile fname
  return rslt

-- | Checks that the size reported by the given BlockDevice (if any)
-- corresponds to its creation parameters
propM_size :: Monad m =>
              BDGeom
           -> Maybe (BlockDevice m)
           -> PropertyM m ()
propM_size _ Nothing    = fail "Invalid BlockDevice"
propM_size g (Just dev) = do
  run $ bdShutdown dev
  assert $
    bdBlockSize dev == bdgSecSz  g &&
    bdNumBlocks dev == bdgSecCnt g

{-
sizeMsg :: String -> BDGeom -> PropertyM IO ()
sizeMsg s g = run $ putStrLn
  $ "Checking geometry on " ++ s ++ "-backed block device ("
  ++ show (bdgSecCnt g) ++ " sectors, "
  ++ show (bdgSecSz g) ++ " bytes/sector, "
  ++ show (bdgSecSz g * bdgSecCnt g)
    ++ " bytes)"
-}

ioBDGeomProp :: (BDGeom -> PropertyM IO ()) -> Property
ioBDGeomProp = monadicIO . forAllM arbBDGeom


