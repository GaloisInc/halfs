module Halfs.Protection(
         UserID
       , GroupID
       , rootUser
       , rootGroup
       )
  where

import Data.Serialize
import Data.Serialize.Put
import Data.Serialize.Get

newtype UserID  = UID Word64

instance Serialize UserID where
  put (UID x) = putWord64be x
  get         = UID `fmap` getWord64be

rootUser :: UserID
rootUser = UID 0

--

newtype GroupID = GID Word64

instance Serialize GroupID where
  put (GID x) = putWord64be x
  get         = GID `fmap` getWord64be

rootGroup :: GroupID
rootGroup = GID 0
