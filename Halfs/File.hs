module Halfs.File
  ( FileHandle
  , createFile
  , fhInode
  , openFilePrim
  )
 where

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Directory
import Halfs.Errors
import Halfs.HalfsState
import Halfs.Inode
import Halfs.Monad
import Halfs.Protection
import Halfs.Types

import System.Device.BlockDevice

--------------------------------------------------------------------------------
-- Types

newtype FileHandle = FH InodeRef

--------------------------------------------------------------------------------
-- File creation and manipulation functions

createFile :: HalfsCapable b t r l m =>
              HalfsState b r l m 
           -> DirHandle r l
           -> FilePath
           -> UserID
           -> GroupID
           -> FileMode
           -> HalfsM m InodeRef
createFile fs parentDH fname usr grp mode = do
  mfileIR <- (fmap . fmap) blockAddrToInodeRef $ lift $ alloc1 (hsBlockMap fs)
  case mfileIR of
    Nothing      -> throwError HE_AllocFailed
    Just fileIR -> do
      let dev = hsBlockDev fs
      n <- lift $ buildEmptyInodeEnc
                    dev
                    RegularFile
                    mode
                    fileIR
                    (dhInode parentDH)
                    usr
                    grp
      lift $ bdWriteBlock dev (inodeRefToBlockAddr fileIR) n 
      addDirEnt parentDH fname fileIR usr grp mode RegularFile
      atomicModifyLockedRscRef (hsNumFileNodes fs) (+1)
      return $ fileIR

openFilePrim :: Monad m => InodeRef -> HalfsM m FileHandle
openFilePrim = return . FH

fhInode :: FileHandle -> InodeRef
fhInode (FH ir) = ir
