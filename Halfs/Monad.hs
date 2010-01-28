module Halfs.Monad
  (
    Halfs(..)
  , HalfsM
  )
  where

import Halfs.BlockMap
import Halfs.Errors
import Halfs.SuperBlock
import System.Device.BlockDevice

type HalfsM m a = m (Either HalfsError a)

data Halfs b r l m = HalfsState {
    hsBlockDev   :: BlockDevice m
  , hsBlockMap   :: BlockMap b r
  , hsSuperBlock :: r SuperBlock
  , hsLock       :: l
  -- TODO: put user/group info here, populate in mount via its interface
  }
