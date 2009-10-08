{-# LANGUAGE MultiParamTypeClasses, FlexibleContexts, FlexibleInstances, FunctionalDependencies #-}
module Halfs.Monad(
         Halfs
       , HalfsM
       , HalfsCapable
       )
  where

import Control.Monad.ST
import Data.Array.IO
import Data.Array.ST
import Data.IORef
import Data.Serialize
import Data.STRef
import Data.Time.Clock
import Data.Word

import Halfs.BlockMap
import Halfs.Classes
import Halfs.Errors
import System.Device.BlockDevice

-- Any monad used in Halfs must implement the following interface:
class (Bitmapped b m, Timed t m, Reffable r m, Lockable l m, Serialize t) =>
   HalfsCapable b t r l m | m -> b t r l

instance HalfsCapable (IOUArray Word64 Bool)   UTCTime IORef     IOLock IO
instance HalfsCapable (STUArray s Word64 Bool) Word64  (STRef s) ()     (ST s)

{-
instance (MArray a Bool m, Timed t m, Reffable r m, Lockable l m, Serialize t) =>
   HalfsCapable a t r l m
-}

type HalfsM m a = m (Either HalfsError a)

data HalfsState a m = HS (a Word64 Bool) (BlockDevice m)

data Halfs b r m l =
  HalfsState {
    blockDev   :: BlockDevice m
  , blockMap   :: BlockMap b r
  , numFiles   :: r Word64
  , lock       :: l
  }

