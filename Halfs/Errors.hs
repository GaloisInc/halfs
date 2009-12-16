module Halfs.Errors
where

data HalfsError =
    HalfsFileNotFound
  | HalfsPathComponentNotFound String
  | HalfsAbsolutePathExpected
  | HalfsUnmountFailed 
  | HalfsMountFailed   RsnHalfsMountFail
  deriving Show

data RsnHalfsMountFail = 
    BadSuperBlock String
  | DirtyUnmount
  deriving Show
           
