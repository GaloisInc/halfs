module Halfs.File
  ( FileHandle
  , openFilePrim
  )
 where

import Halfs.Inode (InodeRef)

newtype FileHandle = FH InodeRef

openFilePrim :: Monad m => InodeRef -> m FileHandle
openFilePrim = return . FH

_foo :: Int
_foo = undefined FH
