module Halfs.Utils where

import qualified Data.Map as M

import Halfs.Classes
import Halfs.Monad

fmapFst :: (a -> b) -> (a, c) -> (b, c)
fmapFst f (x,y) = (f x, y)

fmapSnd :: (b -> c) -> (a, b) -> (a, c)
fmapSnd f (x,y) = (x, f y)

divCeil :: Integral a => a -> a -> a
divCeil a b = (a + (b - 1)) `div` b

unfoldrM :: Monad m => (b -> m (Maybe (a, b))) -> b -> m [a]
unfoldrM f x = do
  r <- f x
  case r of
    Nothing    -> return []
    Just (a,b) -> liftM (a:) $ unfoldrM f b

lookupM :: (Ord k, Monad m) => k -> m (M.Map k v) -> m (Maybe v)
lookupM = liftM . M.lookup

lookupRM :: (Ord k, Reffable r m) => k -> r (M.Map k v) -> m (Maybe v)
lookupRM k = lookupM k . readRef
