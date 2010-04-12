module Halfs.File
  ( FileHandle (..) -- fhInode, fhReadable, fhWritable)
  , createFile
  , fofReadOnly
  , fofWriteOnly
  , fofReadWrite
  , getFHINR_lckd
  , openFilePrim
  , closeFilePrim
  , removeFile
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

-- import Debug.Trace

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
      withDHLock parentDH $ do
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
              Maybe String -- ^ name to remove from parent directory's content
                           -- map (when Nothing, leaves the parent directory's
                           -- content map alone)
           -> InodeRef     -- ^ inr of file to remove
           -> HalfsM b r l m ()
removeFile mfname inr = do
  case mfname of
    Nothing    -> return ()
    Just fname -> do 
      -- Purge the filename from the parent directory
      dhMap <- hasks hsDHMap
      withLockedRscRef dhMap $ \dhMapRef -> do
        pinr <- atomicReadInode inr inoParent
        pdh  <- lookupRM pinr dhMapRef >>= maybe (newDirHandle pinr) return
        rmDirEnt pdh fname

  -- Decrement link count & register deallocation callback
  hbracket (openFilePrim fofReadOnly inr) closeFilePrim $ \fh -> do
    fhMap <- hasks hsFHMap
    withLockedRscRef fhMap $ \fhMapRef -> do
    atomicModifyInode inr  $ \nd       -> do
      when (inoNumLinks nd == 1) $ do
        -- We're removing the last link, so we inject a callback into the fhmap
        -- that will be invoked when all processes have closed the filehandle
        -- associated with inr.  The callback invalidates the filehandle and
        -- releases its inode.  NB: The callback must assume that both the fhmap
        -- and fh locks are held when it is executed.
        mfhData <- lookupRM inr fhMapRef
        case mfhData of 
          Just (_, c, _) -> insertRM inr (fh, c, cb) fhMapRef
            where cb = invalidateFH fh >> freeInode inr
          Nothing        -> error "fh not found for open file, cannot happen"
      return nd{ inoNumLinks = inoNumLinks nd - 1 }
                         
openFilePrim :: HalfsCapable b t r l m =>
                FileOpenFlags -> InodeRef -> HalfsM b r l m (FileHandle r l)
openFilePrim oflags@FileOpenFlags{ openMode = omode } inr = do
  fhMap <- hasks hsFHMap
  withLockedRscRef fhMap $ \fhMapRef -> do
    mfh <- lookupRM inr fhMapRef
    case mfh of
      Just (fh, c, onFinalClose) -> do
        insertRM inr (fh, c + 1, onFinalClose) fhMapRef
        return fh
      Nothing -> do
        fh <- FH (omode /= WriteOnly) (omode /= ReadOnly) oflags
                `fmap` newRef (Just inr)
                `ap`   newLock
        insertRM inr (fh, 1, invalidateFH fh) fhMapRef
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
        Nothing -> return ()
  
        -- Remove FH from the map when the current open count is 1, otherwise
        -- just decrement it.  When FH is removed from the map, execute the
        -- 'onFinalClose' callback from the fhmap (which, by default, simply
        -- invalidates the FH, but may also, e.g., deallocate resources for the
        -- file if a function like rmlink needs to defer resource release).  NB:
        -- onFinalClose may assume that both fh and fhmap locks are held!
        --
        -- TODO: We may want to do this on a per-process basis (e.g., by mapping
        -- an inr to a pid -> open count map) to disallow accidental multiple
        -- closes across multiple processes resulting in incorrect open counts.
        -- At the very least, we should work through the ramifications should
        -- this multiple-close scenario occur. This includes dealing with
        -- per-process FH invalidation as well.  E.g., in the current scheme, if
        -- two processes p1 and p2 have the FH and p1 closes it, FH invalidation
        -- does not occur (it would occur only after p1 and p2 closed the FH)
        -- and so p1 can still use the FH which is not intended.
        --
        Just (_, c, onFinalClose)
          | c == 1    -> deleteRM inr fhMapRef >> onFinalClose
          | otherwise -> insertRM inr (fh, c - 1, onFinalClose) fhMapRef

-- Get file handle's inode reference
getFHINR_lckd :: HalfsCapable b t r l m =>
                 FileHandle r l
              -> HalfsM b r l m InodeRef
getFHINR_lckd fh = 
  -- Precond: (fhLock fh) has been acquried (TODO: can we assert this?)
  readRef (fhInode fh) >>= maybe (throwError HE_InvalidFileHandle) return

invalidateFH :: HalfsCapable b t r l m =>
                FileHandle r l
             -> HalfsM b r l m ()
invalidateFH fh = writeRef (fhInode fh) Nothing

fofReadOnly :: FileOpenFlags
fofReadOnly = FileOpenFlags False False ReadOnly

fofWriteOnly, fofReadWrite :: Bool -> FileOpenFlags
fofWriteOnly app = FileOpenFlags app False WriteOnly
fofReadWrite app = FileOpenFlags app False ReadWrite
