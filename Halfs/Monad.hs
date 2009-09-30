{-# LANGUAGE MultiParamTypeClasses, FlexibleContexts, FlexibleInstances #-}
module Halfs.Monad(
         Halfs
       , HalfsM
       , HalfsCapable
       )
  where

import Data.Array.MArray
import Data.Word

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Errors
import System.Device.BlockDevice

-- Any monad used in Halfs must implement the following interface:
class (MArray a Bool m, Timed t m, Reffable r m, Lockable l m) =>
   HalfsCapable a t r l m

instance (MArray a Bool m, Timed t m, Reffable r m, Lockable l m) =>
   HalfsCapable a t r l m

type HalfsM m a = m (Either HalfsError a)

data HalfsState a m = HS (a Word64 Bool) (BlockDevice m)

data Halfs a r m l =
  HalfsState {
    blockDev   :: BlockDevice m
  , blockMap   :: BlockMap a r
  , numFiles   :: r Word64
  , lock       :: l
  }

