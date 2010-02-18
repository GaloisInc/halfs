module Halfs.Errors
where

import System.FilePath
import Data.Word

data HalfsError =
    HalfsFileNotFound
  | HalfsObjectExists FilePath
  | HalfsPathComponentNotFound String
  | HalfsAbsolutePathExpected
  | HalfsMountFailed RsnHalfsMountFail
  | HalfsUnmountFailed 
  | HalfsAllocFailed
  | HalfsInvalidStreamIndex Word64
  | HalfsDecodeFail_Directory String
  | HalfsDecodeFail_Inode String
  | HalfsDecodeFail_Cont String
  | HalfsDecodeFail_BlockCarrier String
  | HalfsTestFailed String
  | HalfsInternalError String
  deriving (Eq, Show)

data RsnHalfsMountFail = 
    BadSuperBlock String
  | DirtyUnmount
  deriving (Eq, Show)
           
