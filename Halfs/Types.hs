module Halfs.Types
  (
    Halfs(..)
  )
  where

import Data.Map as M

-- import Halfs.Classes
import Halfs.BlockMap            (BlockMap)
import Halfs.SuperBlock          (SuperBlock) 
import System.Device.BlockDevice (BlockDevice)

--------------------------------------------------------------------------------
-- Types

data Halfs b r l m = HalfsState {
    hsBlockDev         :: BlockDevice m
  , hsBlockMap         :: BlockMap b r
  , hsSuperBlock       :: r SuperBlock
  , hsLock             :: l
  -- TODO: put user/group info here, populate in mount via its interface
  }
                   

