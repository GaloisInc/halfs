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
import Halfs.Inode
import Halfs.Monad
import Halfs.Protection
import System.Device.BlockDevice

--------------------------------------------------------------------------------
-- Types

newtype FileHandle = FH InodeRef

--------------------------------------------------------------------------------
-- File creation and manipulation functions

createFile :: HalfsCapable b t r l m =>
              Halfs b r l m 
           -> DirHandle r
           -> FilePath
           -> UserID
           -> GroupID
           -> FileMode
           -> m (Either HalfsError InodeRef)
createFile fs parentDH fname u g mode = do
  mfileIR <- (fmap . fmap) blockAddrToInodeRef $ alloc1 (hsBlockMap fs)
  case mfileIR of
    Nothing       -> return $ Left HalfsAllocFailed
    Just fileIR -> do
      bdWriteBlock (hsBlockDev fs) (inodeRefToBlockAddr fileIR)
        =<< buildEmptyInodeEnc (hsBlockDev fs) fileIR (dhInode parentDH) u g
      addFile parentDH fname fileIR u g mode 
      return $ Right $ fileIR

openFilePrim :: Monad m => InodeRef -> m FileHandle
openFilePrim = return . FH

fhInode :: FileHandle -> InodeRef
fhInode (FH ir) = ir

_foo :: Int
_foo = undefined FH
