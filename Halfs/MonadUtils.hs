module Halfs.MonadUtils
  (
    hasks
  , logMsg
  )
where

import Halfs.Errors
import Halfs.Monad
import Halfs.HalfsState

import System.Device.BlockDevice

type HalfsM b r l m a = HalfsT HalfsError (Maybe (HalfsState b r l m)) m a

logMsg :: Monad m => Maybe (String -> m ()) -> String -> HalfsM b r l m ()
logMsg (Just logger) = lift . logger
logMsg _             = const $ return ()

hasks :: Monad m => (HalfsState b r l m -> a) -> HalfsM b r l m a
hasks f =
  ask >>= maybe (fail "hasks: No valid HalfsState exists") (return . f)
