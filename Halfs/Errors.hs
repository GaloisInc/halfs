module Halfs.Errors
where

data HalfsError =
    HalfsFileNotFound
  | HalfsMountFailed RsnHalfsMountFail
  deriving Show

data RsnHalfsMountFail = 
    BadSuperBlock String
  | DirtyUnmount
  deriving Show
           
