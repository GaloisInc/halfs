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

data Halfs b r m l = HalfsState {
    hsBlockDev   :: BlockDevice m
  , hsBlockMap   :: BlockMap b r
  , hsSuperBlock :: r SuperBlock
  , hsLock       :: l
  }
