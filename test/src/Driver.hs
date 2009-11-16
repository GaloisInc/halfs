module Main where

import Test.QuickCheck (quickCheckResult, Property)
import Test.QuickCheck.Test (isSuccess)
import System.Exit (ExitCode(..), exitFailure, exitWith)

import qualified Tests.BlockDevice as BD

qcProps :: [Property]
qcProps = BD.qcProps

main :: IO ()
main = do
  results <- mapM quickCheckResult qcProps
  if all isSuccess results
    then do
      putStrLn "All tests successful."
      exitWith ExitSuccess
    else do 
      putStrLn "One or more tests failed."
      exitFailure
