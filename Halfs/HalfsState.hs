-- Contains HalfsState, the "global" data structure for tracking
-- filesystem state.

module Halfs.HalfsState
  (
    HalfsState(..)
  )
  where

import Data.Map as M
import Halfs.BlockMap            (BlockMap)
import Halfs.SuperBlock          (SuperBlock) 
import Halfs.Types               (InodeRef, DirHandle)
import System.Device.BlockDevice (BlockDevice)

data HalfsState b r l m = HalfsState {
    hsBlockDev         :: BlockDevice m
  , hsBlockMap         :: BlockMap b r l
  , hsSuperBlock       :: r SuperBlock
  , hsLock             :: l
  , hsDHMap            :: r (M.Map InodeRef (DirHandle r l))
    -- ^ Tracks active directory handles; we probably want to add a
    -- (refcounting?) expiry mechanism so that the size of the map is
    -- bounded.  TODO.
  , hsDHMapLock        :: l
    -- ^ Mutex for the directory handle map                         
  }
