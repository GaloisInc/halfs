module Main
where

--import Foreign.C.Error

import System.Fuse

import Halfs.File (FileHandle)

main :: IO ()
main = fuseMain halfsOps defaultExceptionHandler

halfsOps :: FuseOperations FileHandle
halfsOps = defaultFuseOps
       
       

  
 
