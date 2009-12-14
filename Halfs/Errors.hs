module Halfs.Errors
where

data HalfsError =
    HalfsFileNotFound
  | HalfsUnmountFailed 
  | HalfsMountFailed   RsnHalfsMountFail
  deriving Show

data RsnHalfsMountFail = 
    BadSuperBlock String
  | DirtyUnmount
  deriving Show
           
