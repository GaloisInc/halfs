-- Contains HalfsState, the "global" data structure for tracking
-- filesystem state.

module Halfs.HalfsState
  (
    HalfsState(..)
  )
  where

import Data.Map as M
import Data.Word

import Halfs.BlockMap            (BlockMap)
import Halfs.Errors              (HalfsError)
import Halfs.Monad               (HalfsT)
import Halfs.Protection          (UserID, GroupID)
import Halfs.SuperBlock          (SuperBlock) 
import Halfs.Types               (DirHandle, FileHandle, InodeRef, LockedRscRef)

import System.Device.BlockDevice (BlockDevice)

type HalfsM b r l m a = HalfsT HalfsError (Maybe (HalfsState b r l m)) m a
type FHMapVal b r l m = (FileHandle r l, Word64, HalfsM b r l m ())

data HalfsState b r l m = HalfsState {
    hsBlockDev         :: BlockDevice m
  , hsUserID           :: UserID
  , hsGroupID          :: GroupID
  , hsLogger           :: Maybe (String -> m ())
  , hsSizes            :: (Word64, Word64, Word64, Word64)
    -- ^ explicitly memoized Inode.computeSizes 
  , hsBlockMap         :: BlockMap b r l
  , hsSuperBlock       :: r SuperBlock
  , hsLock             :: l
  , hsNumFileNodes     :: LockedRscRef l r Word64
  , hsDHMap            :: LockedRscRef l r (M.Map InodeRef (DirHandle r l))
    -- ^ Tracks active directory handles; we probably want to add a
    -- (refcounting?) expiry mechanism so that the size of the map is
    -- bounded.  TODO.
  , hsFHMap            :: LockedRscRef l r (M.Map InodeRef (FHMapVal b r l m))
    -- ^ Tracks active file handles, their open counts, and an
    -- on-final-close hook.  This last monadic action is executed
    -- whenever the open count becomes 0 (we do this because, e.g.,
    -- unlinking files can cause deferred resource allocation for
    -- currently-open files).

  , hsInodeLockMap     :: LockedRscRef l r (M.Map InodeRef (l, Word64))
    -- ^ Tracks refcnt'd inode locks. For now, these are single reader/writer
    -- locks.
  }
