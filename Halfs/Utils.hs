module Halfs.Utils where

import Control.Monad

fmapFst :: (a -> b) -> (a, c) -> (b, c)
fmapFst f (x,y) = (f x, y)

divCeil :: Integral a => a -> a -> a
divCeil a b = (a + (b - 1)) `div` b

unfoldrM :: Monad m => (b -> m (Maybe (a, b))) -> b -> m [a]
unfoldrM f x = do
  r <- f x
  case r of
    Nothing    -> return []
    Just (a,b) -> liftM (a:) $ unfoldrM f b

whenOK :: Monad m => m (Either a b) -> (b -> m (Either a c)) -> m (Either a c)
whenOK act f = act >>= either (return . Left) f

