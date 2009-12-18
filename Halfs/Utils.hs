module Halfs.Utils where

import Halfs.Classes

fmapFst :: (a -> b) -> (a, c) -> (b, c)
fmapFst f (x,y) = (f x, y)

modifyRef :: Reffable r m => r a -> (a -> a) -> m ()
modifyRef ref f = readRef ref >>= writeRef ref . f
                  

             