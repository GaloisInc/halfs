module Halfs.Utils where

import qualified Data.Map as M

import Halfs.Classes
import Halfs.Monad
import Halfs.Types

fmapFst :: (a -> b) -> (a, c) -> (b, c)
fmapFst f (x,y) = (f x, y)

fmapSnd :: (b -> c) -> (a, b) -> (a, c)
fmapSnd f (x,y) = (x, f y)

maybe' :: Maybe a -> b -> (a -> b) -> b
maybe' ma = \x y -> maybe x y ma

whenJustM :: Monad m => Maybe a -> (a -> m ()) -> m ()
whenJustM ma f = maybe (return ()) f ma

divCeil :: Integral a => a -> a -> a
divCeil a b = (a + (b - 1)) `div` b

unfoldrM :: Monad m => (b -> m (Maybe (a, b))) -> b -> m [a]
unfoldrM f x = do
  r <- f x
  case r of
    Nothing    -> return []
    Just (a,b) -> liftM (a:) $ unfoldrM f b

unfoldrM_ :: Monad m => (b -> m (Maybe (a, b))) -> b -> m ()
unfoldrM_ f x = unfoldrM f x >> return ()

lookupM :: (Ord k, Monad m) => k -> m (M.Map k v) -> m (Maybe v)
lookupM = liftM . M.lookup

lookupRM :: (Ord k, Reffable r m) => k -> r (M.Map k v) -> m (Maybe v)
lookupRM k = lookupM k . readRef

deleteRM :: (Ord k, Reffable r m) => k -> r (M.Map k v) -> m ()
deleteRM k mapRef = modifyRef mapRef $ M.delete k

insertRM :: (Ord k, Reffable r m) => k -> v -> r (M.Map k v) -> m()
insertRM k v mapRef = modifyRef mapRef $ M.insert k v

lookupDE :: Reffable r m =>
            FilePath -> DirHandle r l -> m (Maybe DirectoryEntry)
lookupDE nm = lookupRM nm . dhContents 

withDHLock :: (HalfsCapable b t r l m) =>
              DirHandle r1 l -> HalfsT err env m a -> HalfsT err env m a
withDHLock = withLock . dhLock
