module Halfs.Utils where

fmapFst :: (a -> b) -> (a, c) -> (b, c)
fmapFst f (x,y) = (f x, y)

divCeil :: Integral a => a -> a -> a
divCeil a b = (a + (b - 1)) `div` b
