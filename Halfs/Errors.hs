module Halfs.Errors
where

import Data.Word

data HalfsError =
    HalfsFileNotFound
  | HalfsDirectoryExists
  | HalfsPathComponentNotFound String
  | HalfsAbsolutePathExpected
  | HalfsUnmountFailed 
  | HalfsMountFailed   RsnHalfsMountFail
  | HalfsAllocFailed
  | HalfsInvalidStreamIndex Word64
  deriving Show

data RsnHalfsMountFail = 
    BadSuperBlock String
  | DirtyUnmount
  deriving Show
           
