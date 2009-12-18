module Halfs.Utils where

import Halfs.Classes

fmapFst :: (a -> b) -> (a, c) -> (b, c)
fmapFst f (x,y) = (f x, y)

             