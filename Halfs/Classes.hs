{-# LANGUAGE MultiParamTypeClasses, GeneralizedNewtypeDeriving,
             FunctionalDependencies, FlexibleContexts,
             FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Halfs.Classes(
         Lockable(..)
       , Reffable(..)
       , TimedT(..)
       , Timed(..)
       , Bitmapped(..)
       , IOLock
       )
 where

import Control.Concurrent.MVar
import Control.Monad.Reader
import Control.Monad.ST
import Data.Array.IO
import Data.Array.ST
import Data.IORef
import Data.Serialize
import Data.Serialize.Get
import Data.Serialize.Put
import Data.STRef
import Data.Time.Clock
import Data.Word

-- ----------------------------------------------------------------------------

-- |A monad implementing Timed implements a monotonic clock that can be read
-- from. One obvious implementation is using the system clock. Another might
-- be a step counter.
class (Monad m, Eq t, Ord t) => Timed t m | m -> t where
  getTime :: m t


-- |This is a monad transformer for the Timed monad, which will work for 2^64
-- steps of an arbitrary underlying monad.
newtype TimedT m a = TimedT { runTimerT :: Word64 -> m a }

ttGetTime :: Monad m => TimedT m Word64
ttGetTime = TimedT $ \ t -> return t

instance Monad m => Monad (TimedT m) where
  return a = TimedT $ \ _ -> return a
  m >>= k  = TimedT $ \ t -> do
               a <- runTimerT m t
               runTimerT (k a) (t + 1)

instance Serialize UTCTime where
  put x = do putWord64be $ fromIntegral $ fromEnum $ utctDay x
             putWord64be $ fromIntegral $ fromEnum $ utctDayTime x
  get   = do day <- (toEnum . fromIntegral) `fmap` getWord64be
             off <- (toEnum . fromIntegral) `fmap` getWord64be
             return $ UTCTime day off

instance Timed UTCTime IO where
  getTime = getCurrentTime

instance Timed Word64 (ST s) where
  getTime = undefined

instance Monad m => Timed Word64 (TimedT m) where
  getTime = ttGetTime

-- ---------------------------------------------------------------------------

-- |A monad implementing Reffable implements a reference type that allows for
-- mutable state.
class Monad m => Reffable r m | m -> r where
  newRef   :: a -> m (r a)
  readRef  :: r a -> m a
  writeRef :: r a -> a -> m ()


instance Reffable (STRef s) (ST s) where
  newRef   = newSTRef
  readRef  = readSTRef
  writeRef = writeSTRef

instance Reffable IORef IO where
  newRef   = newIORef
  readRef  = readIORef
  writeRef = writeIORef

-- ---------------------------------------------------------------------------

-- |A monad implementing locks.
class Monad m => Lockable l m | m -> l where
  newLock  :: m l
  lock     :: l -> m ()
  release  :: l -> m ()

instance Lockable () (ST s) where
  newLock   = return ()
  lock _    = return ()
  release _ = return ()

newtype IOLock = IOLock (MVar ())

instance Lockable IOLock IO where
  newLock            = IOLock `fmap` newMVar ()
  lock (IOLock l)    = takeMVar l
  release (IOLock l) = putMVar l ()

-- ---------------------------------------------------------------------------

class Monad m => Bitmapped b m | m -> b where
  newBitmap :: Word64 -> Bool -> m b
  clearBit  :: b -> Word64 -> m ()
  setBit    :: b -> Word64 -> m ()
  checkBit  :: b -> Word64 -> m Bool

instance Bitmapped (IOUArray Word64 Bool) IO where
  newBitmap s e = newArray (0, s - 1) e
  clearBit b i  = writeArray b i False
  setBit b i    = writeArray b i True
  checkBit b i  = readArray b i

instance Bitmapped (STUArray s Word64 Bool) (ST s) where
  newBitmap s e = newArray (0, s - 1) e
  clearBit b i  = writeArray b i False
  setBit b i    = writeArray b i True
  checkBit b i  = readArray b i

