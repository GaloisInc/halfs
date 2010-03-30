{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, UndecidableInstances #-}

module Halfs.Monad
  (
    module Control.Monad.Error
  , HalfsM
  , HalfsT (runHalfs)
  , atomicModifyLockedRscRef
  , hbracket
  , newLockedRscRef
  , throwErrno
  , withLock
  , withLockM
  , withLockedRscRef
  )
  where

import Control.Monad.Error
import Foreign.C.Error (Errno)

import Halfs.Classes
import Halfs.Errors
import Halfs.Types

newtype HalfsT m a = HalfsT { runHalfs :: m (Either HalfsError a) }
type HalfsM m a    = HalfsT m a

--------------------------------------------------------------------------------
-- Instances

instance (Monad m) => Monad (HalfsT m) where
  m >>= k = HalfsT $ runHalfs m >>= either (return . Left) (runHalfs . k)
  return  = HalfsT . return . Right

instance (Monad m) => Functor (HalfsT m) where
  fmap f fa =
    HalfsT $ runHalfs fa >>= either (return . Left) (return . Right . f)

instance MonadTrans HalfsT where
  lift = HalfsT . (=<<) (return . Right)

instance (Monad m) => MonadError HalfsError (HalfsT m) where
  throwError e = HalfsT $ return $ Left e
  catchError act h =
    HalfsT $ do
      eea <- runHalfs act
      case eea of
        Left  e -> runHalfs (h e)
        Right a -> return $ Right a

instance Reffable r m => Reffable r (HalfsT m) where
  newRef     = lift . newRef     
  readRef    = lift . readRef    
  writeRef r = lift . writeRef r 

instance Bitmapped b m => Bitmapped b (HalfsT m) where
  newBitmap w = lift . newBitmap w 
  clearBit b  = lift . clearBit b
  setBit b    = lift . setBit b
  checkBit b  = lift . checkBit b
  toList      = lift . toList

instance Lockable l m => Lockable l (HalfsT m) where
  newLock = lift newLock
  lock    = lift . lock
  release = lift . release

instance Timed t m => Timed t (HalfsT m) where
  getTime   = lift getTime
  toCTime   = lift . toCTime
  fromCTime = lift . fromCTime

instance Threaded m => Threaded (HalfsT m) where
  getThreadId = lift getThreadId

--------------------------------------------------------------------------------
-- Utility functions specific to the Halfs monad

hbracket :: Monad m =>
            HalfsM m a         -- ^ before (\"acquisition\")
         -> (a -> HalfsM m b)  -- ^ after  (\"release\")
         -> (a -> HalfsM m c)  -- ^ bracketed computation
         -> HalfsM m c         -- ^ result of bracketed computation
hbracket before after act = do
  a <- before
  r <- act a `catchError` \e -> after a >> throwError e
  _ <- after a
  return r

withLock :: HalfsCapable b t r l m =>
            l -> HalfsM m a -> HalfsM m a
withLock l act = do
  lock l
  res <- act `catchError` \e -> release l >> throwError e
  release l
  return res

withLockM :: (Monad m, Lockable l m) =>
             l -> m a -> m a
withLockM l act = do
  lock l
  res <- act
  release l
  return res
         
throwErrno :: Monad m => Errno -> HalfsError -> HalfsM m a
throwErrno en = throwError . (`HE_ErrnoAnnotated` en)

--------------------------------------------------------------------------------
-- Locked resource reference utility functions

newLockedRscRef :: (Lockable l m, Functor m, Reffable r m) =>
                   rsc
                -> m (LockedRscRef l r rsc)
newLockedRscRef initial = LockedRscRef `fmap` newLock `ap` newRef initial

withLockedRscRef :: HalfsCapable b t r l m =>
                    LockedRscRef l r rsc
                 -> (r rsc -> HalfsM m rslt)
                 -> HalfsM m rslt
withLockedRscRef lr f = withLock (lrLock lr) $ f (lrRsc lr)

atomicModifyLockedRscRef :: HalfsCapable b t r l m =>
                            LockedRscRef l r rsc
                         -> (rsc -> rsc)
                         -> HalfsM m ()
atomicModifyLockedRscRef lr f = withLockedRscRef lr (`modifyRef` f)
