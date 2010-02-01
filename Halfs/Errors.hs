module Halfs.Errors
where

import System.FilePath
import Data.Word

data HalfsError =
    HalfsFileNotFound
  | HalfsFileExists FilePath
  | HalfsDirectoryExists
  | HalfsPathComponentNotFound String
  | HalfsAbsolutePathExpected
  | HalfsMountFailed RsnHalfsMountFail
  | HalfsUnmountFailed 
  | HalfsAllocFailed
  | HalfsInvalidStreamIndex Word64
  | HalfsDecodeFail_Directory String
  | HalfsDecodeFail_Inode String
  | HalfsTestFailed String
  deriving Show

data RsnHalfsMountFail = 
    BadSuperBlock String
  | DirtyUnmount
  deriving Show
           
