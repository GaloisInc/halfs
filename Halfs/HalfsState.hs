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
import Halfs.SuperBlock          (SuperBlock) 
import Halfs.Types               (DirHandle, InodeRef, LockedRscRef)

import System.Device.BlockDevice (BlockDevice)

data HalfsState b r l m = HalfsState {
    hsBlockDev         :: BlockDevice m
  , hsBlockMap         :: BlockMap b r l
  , hsSuperBlock       :: r SuperBlock
  , hsLock             :: l
  , hsNumFileNodes     :: LockedRscRef l r Word64 
  , hsDHMap            :: LockedRscRef l r (M.Map InodeRef (DirHandle r l))
    -- ^ Tracks active directory handles; we probably want to add a
    -- (refcounting?) expiry mechanism so that the size of the map is
    -- bounded.  TODO.
  , hsInodeLockMap     :: LockedRscRef l r (M.Map InodeRef (l, Word64))
    -- ^ Tracks refcnt'd inode locks. For now, these are single reader/writer
    -- locks.
  , hsLogger           :: Maybe (String -> m ())
  }
