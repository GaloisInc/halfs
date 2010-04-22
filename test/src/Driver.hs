module Main where

import Test.QuickCheck (Args, Property, quickCheckWithResult)
import Test.QuickCheck.Test (isSuccess)
import System.Exit (ExitCode(..), exitFailure, exitWith)

import qualified Tests.BlockDevice as BD
import qualified Tests.BlockMap    as BM
import qualified Tests.CoreAPI     as CA  
import qualified Tests.Inode       as IN
import qualified Tests.Serdes      as SD

qcProps :: [(Args, Property)]
qcProps =
--   BD.qcProps True -- run in "quick" mode for Block Devices
--   ++
--   BM.qcProps True -- run in "quick" mode for Block Map
--   ++
--   SD.qcProps True -- run in "quick" mode for Serdes
--   ++
  IN.qcProps True -- run in "quick" mode for Inode
--   ++
--   CA.qcProps True -- run in "quick" mode for CoreAPI

main :: IO ()
main = do
  results <- mapM (uncurry quickCheckWithResult) qcProps
  if all isSuccess results
   then do
     putStrLn "All tests successful."
     exitWith ExitSuccess
   else do 
     putStrLn "One or more tests failed."
     exitFailure
