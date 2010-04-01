module Halfs.File
  ( FileHandle (fhInode, fhReadable, fhWritable)
  , createFile
  , fofReadOnly
  , fofWriteOnly
  , fofReadWrite
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

data FileHandle = FH
  { fhReadable :: Bool
  , fhWritable :: Bool 
  , _fhFlags   :: FileOpenFlags
  , fhInode    :: InodeRef
  }
  deriving Show

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
  mfileIR <- fmap blockAddrToInodeRef `fmap` lift (alloc1 $ hsBlockMap fs)
  case mfileIR of
    Nothing      -> throwError HE_AllocFailed
    Just fileIR -> do
      let dev = hsBlockDev fs
      withLock (dhLock parentDH) $ do
      pIR <- getDHINR_lckd parentDH               
      n   <- lift $ buildEmptyInodeEnc
                      dev
                      RegularFile
                      mode
                      fileIR
                      pIR
                      usr
                      grp
      lift $ bdWriteBlock dev (inodeRefToBlockAddr fileIR) n 
      addDirEnt_lckd parentDH fname fileIR usr grp mode RegularFile
      atomicModifyLockedRscRef (hsNumFileNodes fs) (+1)
      return $ fileIR

openFilePrim :: Monad m => FileOpenFlags -> InodeRef -> HalfsM m FileHandle
openFilePrim oflags@FileOpenFlags{ openMode = omode } inr = 
  -- TODO: lock for writing / mutex access for deletion
  return $ FH (omode /= WriteOnly) (omode /= ReadOnly) oflags inr

fofReadOnly :: FileOpenFlags
fofReadOnly = FileOpenFlags False False ReadOnly

fofWriteOnly, fofReadWrite :: Bool -> FileOpenFlags
fofWriteOnly app = FileOpenFlags app False WriteOnly
fofReadWrite app = FileOpenFlags app False ReadWrite
