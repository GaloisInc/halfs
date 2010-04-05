{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, UndecidableInstances #-}

module Halfs.Monad
  (
    module Control.Monad.Reader
  , MonadError(..)
  , HalfsT
  , atomicModifyLockedRscRef
  , hbracket
  , newLockedRscRef
  , runHalfs
  , runHalfsNoEnv
  , withLock
  , withLockM
  , withLockedRscRef
  )
  where

import Control.Monad.Error
import Control.Monad.Reader

import Halfs.Classes
import Halfs.Types

newtype HalfsT err env m a = HalfsT { runHalfsT :: ReaderT env m (Either err a) }

runHalfs :: env -> HalfsT err (Maybe env) m a -> m (Either err a)
runHalfs = flip (runReaderT . runHalfsT) . Just

runHalfsNoEnv :: HalfsT err (Maybe env) m a -> m (Either err a)
runHalfsNoEnv = flip (runReaderT . runHalfsT) Nothing

--------------------------------------------------------------------------------
-- Instances

instance (Monad m) => Monad (HalfsT err env m) where
  m >>= k = HalfsT $ runHalfsT m >>= either (return . Left) (runHalfsT . k)
  return  = HalfsT . return . Right

instance (Monad m) => Functor (HalfsT err env m) where
  fmap f fa =
    HalfsT $ runHalfsT fa >>= either (return . Left) (return . Right . f)

instance MonadTrans (HalfsT err env) where
  lift = HalfsT . lift . liftM Right
    -- HalfsT . (=<<) (return . Right)

instance (Monad m) => MonadError err (HalfsT err env m) where
  throwError e = HalfsT $ return $ Left e
  catchError act h =
    HalfsT $ do
      eea <- runHalfsT act
      case eea of
        Left  e -> runHalfsT (h e)
        Right a -> return $ Right a

instance Monad m => MonadReader r (HalfsT err r m) where
  ask       = HalfsT $ ReaderT $ return . Right
  local f m = HalfsT $ ReaderT $ \r -> runReaderT (runHalfsT m) (f r)
  
instance Reffable r m => Reffable r (HalfsT err env m) where
  newRef     = lift . newRef     
  readRef    = lift . readRef    
  writeRef r = lift . writeRef r 

instance Bitmapped b m => Bitmapped b (HalfsT err env m) where
  newBitmap w = lift . newBitmap w 
  clearBit b  = lift . clearBit b
  setBit b    = lift . setBit b
  checkBit b  = lift . checkBit b
  toList      = lift . toList

instance Lockable l m => Lockable l (HalfsT err env m) where
  newLock = lift newLock
  lock    = lift . lock
  release = lift . release

instance Timed t m => Timed t (HalfsT err env m) where
  getTime   = lift getTime
  toCTime   = lift . toCTime
  fromCTime = lift . fromCTime

instance Threaded m => Threaded (HalfsT err env m) where
  getThreadId = lift getThreadId

--------------------------------------------------------------------------------
-- Utility functions specific to the Halfs monad

hbracket :: Monad m =>
            HalfsT err env m a         -- ^ before (\"acquisition\")
         -> (a -> HalfsT err env m b)  -- ^ after  (\"release\")
         -> (a -> HalfsT err env m c)  -- ^ bracketed computation
         -> HalfsT err env m c         -- ^ result of bracketed computation
hbracket before after act = do
  a <- before
  r <- act a `catchError` \e -> after a >> throwError e
  _ <- after a
  return r

withLock :: HalfsCapable b t r l m =>
            l -> HalfsT err env m a -> HalfsT err env m a
withLock l act = hbracket (lock l) (const $ release l) (const act)

withLockM :: (Monad m, Lockable l m) =>
             l -> m a -> m a
withLockM l act = do
  lock l
  res <- act
  release l
  return res
         
--------------------------------------------------------------------------------
-- Locked resource reference utility functions

newLockedRscRef :: (Lockable l m, Functor m, Reffable r m) =>
                   rsc
                -> m (LockedRscRef l r rsc)
newLockedRscRef initial = LockedRscRef `fmap` newLock `ap` newRef initial

withLockedRscRef :: HalfsCapable b t r l m =>
                    LockedRscRef l r rsc
                 -> (r rsc -> HalfsT err env m rslt)
                 -> HalfsT err env m rslt
withLockedRscRef lr f = withLock (lrLock lr) $ f (lrRsc lr)

atomicModifyLockedRscRef :: HalfsCapable b t r l m =>
                            LockedRscRef l r rsc
                         -> (rsc -> rsc)
                         -> HalfsT err env m ()
atomicModifyLockedRscRef lr f = withLockedRscRef lr (`modifyRef` f)
