{-# OPTIONS_GHC -fno-warn-orphans #-}

module Halfs.Errors
where

import Foreign.C.Error (Errno)

import System.FilePath
import Data.Word

import Halfs.Types

data HalfsError =
    HE_FileNotFound
  | HE_UnexpectedFileType FileType FilePath
  | HE_ObjectExists FilePath
  | HE_PathComponentNotFound String
  | HE_AbsolutePathExpected
  | HE_MountFailed RsnHalfsMountFail
  | HE_UnmountFailed 
  | HE_AllocFailed
  | HE_InvalidStreamIndex Word64
  | HE_DecodeFail_Directory String
  | HE_DecodeFail_Inode String
  | HE_DecodeFail_Cont String
  | HE_DecodeFail_BlockCarrier String
  | HE_TestFailed String
  | HE_InternalError String
  | HE_ErrnoAnnotated HalfsError Errno
  deriving (Eq, Show)

data RsnHalfsMountFail = 
    BadSuperBlock String
  | DirtyUnmount
  deriving (Eq, Show)
           
instance Show Errno where
  show _ = "<errno>"
