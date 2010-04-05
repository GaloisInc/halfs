module Halfs.File
  ( FileHandle (..) -- fhInode, fhReadable, fhWritable)
  , createFile
  , fofReadOnly
  , fofWriteOnly
  , fofReadWrite
  , getFHINR_lckd
  , openFilePrim
  , closeFilePrim
  )
 where

import qualified Data.Map as M

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Directory
import Halfs.Errors
import Halfs.HalfsState
import Halfs.Inode
import Halfs.Monad
import Halfs.MonadUtils
import Halfs.Protection
import Halfs.Types
import Halfs.Utils

import System.Device.BlockDevice

type HalfsM b r l m a = HalfsT HalfsError (Maybe (HalfsState b r l m)) m a

--------------------------------------------------------------------------------
-- File creation and manipulation functions

createFile :: HalfsCapable b t r l m =>
              DirHandle r l
           -> FilePath
           -> UserID
           -> GroupID
           -> FileMode
           -> HalfsM b r l m InodeRef
createFile parentDH fname usr grp mode = do
  dev <- hasks hsBlockDev
  bm  <- hasks hsBlockMap
  mfileIR <- fmap blockAddrToInodeRef `fmap` lift (alloc1 bm)
  case mfileIR of
    Nothing      -> throwError HE_AllocFailed
    Just fileIR -> do
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
      numNodesRsc <- hasks hsNumFileNodes
      atomicModifyLockedRscRef numNodesRsc (+1)
      return $ fileIR

removeFile :: HalfsCapable b t r l m =>
              String   -- ^ name of file to remove from parent directory
           -> InodeRef -- ^ inr of file to remove
           -> HalfsM b r l m ()
removeFile = error "removeFile NYI" -- HERE!

-- Get file handle's inode reference
getFHINR_lckd :: HalfsCapable b t r l m =>
                 FileHandle r l
              -> HalfsM b r l m InodeRef
getFHINR_lckd fh = 
  -- Precond: (fhLock fh) has been acquried (TODO: can we assert this?)
  readRef (fhInode fh) >>= maybe (throwError HE_InvalidFileHandle) return

openFilePrim :: HalfsCapable b t r l m =>
                FileOpenFlags -> InodeRef -> HalfsM b r l m (FileHandle r l)
openFilePrim oflags@FileOpenFlags{ openMode = omode } inr = do
  fhMap <- hasks hsFHMap
  withLockedRscRef fhMap $ \fhMapRef -> do
    mfh <- lookupRM inr fhMapRef
    case mfh of
      Just (fh, c, onFinalClose) -> do
        modifyRef fhMapRef (M.insert inr (fh, c + 1, onFinalClose))
        return fh
      Nothing         -> do
        fh <- FH (omode /= WriteOnly) (omode /= ReadOnly) oflags
                `fmap` newRef (Just inr)
                `ap`   newLock
        modifyRef fhMapRef $ M.insert inr (fh, 1, return ())
        return fh

closeFilePrim :: HalfsCapable b t r l m =>
                 FileHandle r l
              -> HalfsM b r l m ()
closeFilePrim fh = do
  fhMap <- hasks hsFHMap
  withLock (fhLock fh) $ do 
  withLockedRscRef fhMap $ \fhMapRef -> do
    inr <- getFHINR_lckd fh 
    mfh <- lookupRM inr fhMapRef
    case mfh of
      -- If the FH isn't in the fhmap, we don't have to do
      -- anything (e.g., it has already been closed.)
      Nothing                  -> return ()

      -- Remove FH from the map when the current open count is 1, otherwise just
      -- decrement it.  When FH is removed from the map, execute the
      -- 'onFinalClose' hook from the fhmap (which, e.g., may deallocate
      -- resources for the file if a function like rmlink needed to defer
      -- deallocation).
      --
      -- TODO: Note that we may want to do this on a per-process basis (e.g., by
      -- mapping an inr to a pid -> open count map) to disallow accidental
      -- multiple closes across multiple processes resulting in incorrect open
      -- counts.  At the very least, we should work through the ramifications
      -- should this multiple-close scenario occur.
      Just (_, c, onFinalClose)
        | c == 1    ->
            modifyRef fhMapRef (M.delete inr) >> onFinalClose
        | otherwise ->
            modifyRef fhMapRef $ M.insert inr (fh, c - 1, onFinalClose)

fofReadOnly :: FileOpenFlags
fofReadOnly = FileOpenFlags False False ReadOnly

fofWriteOnly, fofReadWrite :: Bool -> FileOpenFlags
fofWriteOnly app = FileOpenFlags app False WriteOnly
fofReadWrite app = FileOpenFlags app False ReadWrite
