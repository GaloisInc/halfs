module Halfs.Utils where

fmapFst :: (a -> b) -> (a, c) -> (b, c)
fmapFst f (x,y) = (f x, y)
  


