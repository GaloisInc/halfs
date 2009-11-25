module Main where

import Test.QuickCheck (Args, Property, quickCheckWithResult)
import Test.QuickCheck.Test (isSuccess)
import System.Exit (ExitCode(..), exitFailure, exitWith)

import qualified Tests.BlockDevice as BD

qcProps :: [(Args, Property)]
qcProps = BD.qcProps True -- "quick" mode for Block Devices

main :: IO ()
main = do
  results <- mapM (\(args,p) -> quickCheckWithResult args p) qcProps
  if all isSuccess results
    then do
      putStrLn "All tests successful."
      exitWith ExitSuccess
    else do 
      putStrLn "One or more tests failed."
      exitFailure
