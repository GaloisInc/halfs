module Halfs.Protection(
         UserID
       , GroupID
       , rootUser
       , rootGroup
       )
  where

newtype UserID  = UID Int
newtype GroupID = GID Int

rootUser :: UserID
rootUser = UID 0

rootGroup :: GroupID
rootGroup = GID 0
