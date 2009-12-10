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

-- import Debug.Trace

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
  put x = do
    let day = fromIntegral $ fromEnum $ utctDay x
        off = fromIntegral $ fromEnum $ utctDayTime x

-- fromEnum (1 * 1000000000000 :: Integer) etc. (or just fromEnum $
-- secondsToDiffTime 1) is the key to why this is messed up here,
-- because fromEnum on DiffTime just invokes fromEnum on the underlying
-- Data.Fixed Integer.

-- round (10**12 :: Double) gets us the desired pico resolution
-- precision as a constant Intege, should we need it to do calcs like
-- what occur in showFixed...

-- So this is actually quite easy, if we can just get the value as an
-- Integer...but it's not looking like it'll work that way :(

--    trace ("PUT:utctDay x = " ++ show (utctDay x) ++ ", day = " ++ show day) $ do
--    trace ("PUT:utctDayTime x= " ++ show (utctDayTime x) ++ ", off = " ++ show off) $ do

    putWord64be day
    putWord64be off 

  get = do
    day <- (toEnum . fromIntegral) `fmap` getWord64be
    off <- (toEnum . fromIntegral) `fmap` getWord64be
--    trace ("GET: day = " ++ show day) $ do
--    trace ("GET: off = " ++ show off) $ do                         
    return $ UTCTime day off

chopZeros :: Integer -> String
chopZeros 0 = ""
chopZeros a | mod a 10 == 0 = chopZeros (div a 10)
chopZeros a = show a

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
  toList    :: b -> m [Bool]


instance Bitmapped (IOUArray Word64 Bool) IO where
  newBitmap s e = newArray (0, s - 1) e
  clearBit b i  = writeArray b i False
  setBit b i    = writeArray b i True
  checkBit      = readArray
  toList        = getElems

instance Bitmapped (STUArray s Word64 Bool) (ST s) where
  newBitmap s e = newArray (0, s - 1) e
  clearBit b i  = writeArray b i False
  setBit b i    = writeArray b i True
  checkBit      = readArray
  toList        = getElems

