{-# LANGUAGE MultiParamTypeClasses, GeneralizedNewtypeDeriving,
             FunctionalDependencies, FlexibleContexts,
             FlexibleInstances #-}
module Halfs.Classes(
         Lockable(..)
       , Reffable(..)
       , TimedT(..)
       , Timed(..)
       )
 where

import Control.Concurrent.MVar
import Control.Monad.Reader
import Control.Monad.ST
import Data.Array.MArray
import Data.Binary
import Data.Binary.Put
import Data.Binary.Get
import Data.IORef
import Data.STRef
import Data.Time.Clock

-- ----------------------------------------------------------------------------

-- |A monad implementing Timed implements a monotonic clock that can be read
-- from. One obvious implementation is using the system clock. Another might
-- be a step counter.
class (Monad m, Binary t, Eq t, Ord t) => Timed t m | m -> t where
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

instance Timed UTCTime IO where
  getTime = getCurrentTime

instance Monad m => Timed Word64 (TimedT m) where
  getTime = ttGetTime

instance Binary UTCTime where
  get   = do d <- (toEnum . fromIntegral) `fmap` getWord64host
             s <- (toEnum . fromIntegral) `fmap` getWord64host
             return $ UTCTime d (fromIntegral s)
  put t = do putWord64host (fromIntegral $ fromEnum $ utctDay t)
             putWord64host (fromIntegral $ fromEnum $ utctDayTime t)

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


