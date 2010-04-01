{-# LANGUAGE FlexibleContexts #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Halfs.Errors
where

import Control.Monad.Error (MonadError, throwError)
import Data.Word
import Foreign.C.Error
import System.FilePath

import Halfs.Types

data HalfsError =
    HE_AbsolutePathExpected
  | HE_AllocFailed
  | HE_BadFileHandleForRead
  | HE_BadFileHandleForWrite
  | HE_DecodeFail_BlockCarrier String
  | HE_DecodeFail_Cont String
  | HE_DecodeFail_Directory String
  | HE_DecodeFail_Inode String
  | HE_DirectoryHandleNotFound
  | HE_ErrnoAnnotated HalfsError Errno
  | HE_FileNotFound
  | HE_InternalError String
  | HE_InvalidStreamIndex Word64
  | HE_InvalidDirHandle
  | HE_MountFailed RsnHalfsMountFail
  | HE_ObjectExists FilePath
  | HE_ObjectDNE FilePath
  | HE_PathComponentNotFound String
  | HE_TestFailed String
  | HE_UnexpectedFileType FileType FilePath
  | HE_UnmountFailed 
  deriving (Eq, Show)

data RsnHalfsMountFail = 
    BadSuperBlock String
  | DirtyUnmount
  deriving (Eq, Show)
           
annErrno :: MonadError HalfsError m => HalfsError -> Errno -> m a
e `annErrno` errno = throwError (e `HE_ErrnoAnnotated` errno)

-- TODO: template haskell to make this a bit cleaner?
instance Show Errno where
  show en | en == eOK             = "EOK"
          | en == e2BIG           = "E2BIG"
          | en == eACCES          = "EACCES"
          | en == eADDRINUSE      = "EADDRINUSE"
          | en == eADDRNOTAVAIL   = "EADDRNOTAVAIL"
          | en == eADV            = "EADV"
          | en == eAFNOSUPPORT    = "EAFNOSUPPORT"
          | en == eAGAIN          = "EAGAIN"
          | en == eALREADY        = "EALREADY"
          | en == eBADF           = "EBADF"
          | en == eBADMSG         = "EBADMSG"
          | en == eBADRPC         = "EBADRPC"
          | en == eBUSY           = "EBUSY"
          | en == eCHILD          = "ECHILD"
          | en == eCOMM           = "ECOMM"
          | en == eCONNABORTED    = "ECONNABORTED"
          | en == eCONNREFUSED    = "ECONNREFUSED"
          | en == eCONNRESET      = "ECONNRESET"
          | en == eDEADLK         = "EDEADLK"
          | en == eDESTADDRREQ    = "EDESTADDRREQ"
          | en == eDIRTY          = "EDIRTY"
          | en == eDOM            = "EDOM"
          | en == eDQUOT          = "EDQUOT"
          | en == eEXIST          = "EEXIST"
          | en == eFAULT          = "EFAULT"
          | en == eFBIG           = "EFBIG"
          | en == eFTYPE          = "EFTYPE"
          | en == eHOSTDOWN       = "EHOSTDOWN"
          | en == eHOSTUNREACH    = "EHOSTUNREACH"
          | en == eIDRM           = "EIDRM"
          | en == eILSEQ          = "EILSEQ"
          | en == eINPROGRESS     = "EINPROGRESS"
          | en == eINTR           = "EINTR"
          | en == eINVAL          = "EINVAL"
          | en == eIO             = "EIO"
          | en == eISCONN         = "EISCONN"
          | en == eISDIR          = "EISDIR"
          | en == eLOOP           = "ELOOP"
          | en == eMFILE          = "EMFILE"
          | en == eMLINK          = "EMLINK"
          | en == eMSGSIZE        = "EMSGSIZE"
          | en == eMULTIHOP       = "EMULTIHOP"
          | en == eNAMETOOLONG    = "ENAMETOOLONG"
          | en == eNETDOWN        = "ENETDOWN"
          | en == eNETRESET       = "ENETRESET"
          | en == eNETUNREACH     = "ENETUNREACH"
          | en == eNFILE          = "ENFILE"
          | en == eNOBUFS         = "ENOBUFS"
          | en == eNODATA         = "ENODATA"
          | en == eNODEV          = "ENODEV"
          | en == eNOENT          = "ENOENT"
          | en == eNOEXEC         = "ENOEXEC"
          | en == eNOLCK          = "ENOLCK"
          | en == eNOLINK         = "ENOLINK"
          | en == eNOMEM          = "ENOMEM"
          | en == eNOMSG          = "ENOMSG"
          | en == eNONET          = "ENONET"
          | en == eNOPROTOOPT     = "ENOPROTOOPT"
          | en == eNOSPC          = "ENOSPC"
          | en == eNOSR           = "ENOSR"
          | en == eNOSTR          = "ENOSTR"
          | en == eNOSYS          = "ENOSYS"
          | en == eNOTBLK         = "ENOTBLK"
          | en == eNOTCONN        = "ENOTCONN"
          | en == eNOTDIR         = "ENOTDIR"
          | en == eNOTEMPTY       = "ENOTEMPTY"
          | en == eNOTSOCK        = "ENOTSOCK"
          | en == eNOTTY          = "ENOTTY"
          | en == eNXIO           = "ENXIO"
          | en == eOPNOTSUPP      = "EOPNOTSUPP"
          | en == ePERM           = "EPERM"
          | en == ePFNOSUPPORT    = "EPFNOSUPPORT"
          | en == ePIPE           = "EPIPE"
          | en == ePROCLIM        = "EPROCLIM"
          | en == ePROCUNAVAIL    = "EPROCUNAVAIL"
          | en == ePROGMISMATCH   = "EPROGMISMATCH"
          | en == ePROGUNAVAIL    = "EPROGUNAVAIL"
          | en == ePROTO          = "EPROTO"
          | en == ePROTONOSUPPORT = "EPROTONOSUPPORT"
          | en == ePROTOTYPE      = "EPROTOTYPE"
          | en == eRANGE          = "ERANGE"
          | en == eREMCHG         = "EREMCHG"
          | en == eREMOTE         = "EREMOTE"
          | en == eROFS           = "EROFS"
          | en == eRPCMISMATCH    = "ERPCMISMATCH"
          | en == eRREMOTE        = "ERREMOTE"
          | en == eSHUTDOWN       = "ESHUTDOWN"
          | en == eSOCKTNOSUPPORT = "ESOCKTNOSUPPORT"
          | en == eSPIPE          = "ESPIPE"
          | en == eSRCH           = "ESRCH"
          | en == eSRMNT          = "ESRMNT"
          | en == eSTALE          = "ESTALE"
          | en == eTIME           = "ETIME"
          | en == eTIMEDOUT       = "ETIMEDOUT"
          | en == eTOOMANYREFS    = "ETOOMANYREFS"
          | en == eTXTBSY         = "ETXTBSY"
          | en == eUSERS          = "EUSERS"
          | en == eWOULDBLOCK     = "EWOULDBLOCK"
          | en == eXDEV           = "EXDEV"
          | otherwise             = "<unknown errno>"
