module Halfs.MonadUtils
  (
    hasks
  , hlocal
  , logMsg
  )
where

import Data.Maybe (fromJust)
  
import Halfs.Errors
import Halfs.Monad
import Halfs.HalfsState

type HalfsM b r l m a = HalfsT HalfsError (Maybe (HalfsState b r l m)) m a

logMsg :: Monad m => String -> HalfsM b r l m ()
logMsg msg = hasks hsLogger >>= maybe (return ()) (\logger -> lift $ logger msg)

hasks :: Monad m => (HalfsState b r l m -> a) -> HalfsM b r l m a
hasks f =
  ask >>= maybe (fail "hasks: No valid HalfsState exists") (return . f)

hlocal :: Monad m =>
          (HalfsState b r l m -> HalfsState b r l m)
       -> HalfsM b r l m a
       -> HalfsM b r l m a
hlocal f m = local (Just . f . fromJust) m 
