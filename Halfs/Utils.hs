module Halfs.Utils where

import Halfs.Classes 
import Halfs.Monad

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
