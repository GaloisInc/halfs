{-# OPTIONS_GHC -fno-warn-orphans #-}

module Halfs.Errors
where

import Foreign.C.Error (Errno)

import System.FilePath
import Data.Word

import Halfs.Types

data HalfsError =
    HE_AbsolutePathExpected
  | HE_AllocFailed
  | HE_DecodeFail_BlockCarrier String
  | HE_DecodeFail_Cont String
  | HE_DecodeFail_Directory String
  | HE_DecodeFail_Inode String
  | HE_DirectoryHandleNotFound
  | HE_ErrnoAnnotated HalfsError Errno
  | HE_FileNotFound
  | HE_InternalError String
  | HE_InvalidStreamIndex Word64
  | HE_MountFailed RsnHalfsMountFail
  | HE_ObjectExists FilePath
  | HE_PathComponentNotFound String
  | HE_TestFailed String
  | HE_UnexpectedFileType FileType FilePath
  | HE_UnmountFailed 
  deriving (Eq, Show)

data RsnHalfsMountFail = 
    BadSuperBlock String
  | DirtyUnmount
  deriving (Eq, Show)
           
instance Show Errno where
  show _ = "<errno>"
