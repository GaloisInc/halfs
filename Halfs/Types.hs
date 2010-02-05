-- This module contains types and instances common to most of Halfs

{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Halfs.Types
where

import Control.Applicative
import Control.Monad
import Data.Bits
import qualified Data.Map as M
import Data.List (sort)
import Data.Serialize
import Data.Serialize.Get (getWord64be)
import Data.Serialize.Put (putWord64be)
import Data.Word

import Halfs.Protection  

--------------------------------------------------------------------------------
-- Common Inode Types

-- We store Inode reference as simple Word64, newtype'd in case we either decide
-- to do something more fancy or just to make the types a bit more clear.
--
-- At this point, we assume an Inode reference is equal to its block address,
-- and we fix Inode references as Word64s. Note that if you change the
-- underlying field size of an InodeRef, you *really* (!)  need to change
-- 'inodeRefSize', below.
--

newtype InodeRef = IR Word64
  deriving (Eq, Ord, Num, Show, Integral, Enum, Real)

instance Serialize InodeRef where
  put (IR x) = putWord64be x
  get        = IR `fmap` getWord64be

-- | The size of an Inode reference in bytes
inodeRefSize :: Word64
inodeRefSize = 8

--------------------------------------------------------------------------------
-- Common Directory and File Types

-- File names are arbitrary-length, null-terminated strings.  Valid file names
-- are guaranteed to not include null or the System.FilePath.pathSeparator
-- character.

data DirectoryEntry = DirEnt {
    deName  :: String
  , deInode :: InodeRef
  , deUser  :: UserID
  , deGroup :: GroupID
  , deMode  :: FileMode
  , deType  :: FileType
  }
  deriving (Show, Eq)

data DirHandle r l = DirHandle {
    dhInode       :: InodeRef
  , dhContents    :: r (M.Map FilePath DirectoryEntry)
  , dhState       :: r DirectoryState
  , dhLock        :: l
  }

data AccessRight = Read | Write | Execute
  deriving (Show, Eq, Ord)

data DirectoryState = Clean | OnlyAdded | OnlyDeleted | VeryDirty
  deriving (Show, Eq)

data FileMode = FileMode {
    fmOwnerPerms :: [AccessRight]
  , fmGroupPerms :: [AccessRight]
  , fmUserPerms  :: [AccessRight]
  }
  deriving (Show)

data FileType = RegularFile | Directory | Symlink
  deriving (Show, Eq)

data FileStat t = FileStat {
    fsInode      :: InodeRef
  , fsType       :: FileType
  , fsMode       :: FileMode
  , fsNumLinks   :: Word64
  , fsUID        :: UserID
  , fsGID        :: GroupID
  , fsSize       :: Word64
  , fsNumBlocks  :: Word64
  , fsAccessTime :: t -- Time of last access
  , fsModTime    :: t -- Time of last data modification
  , fsChangeTime :: t -- Time of last status change
  }


--------------------------------------------------------------------------------
-- Misc Instances

instance Serialize DirectoryEntry where
  put de = do
    put $ deName  de
    put $ deInode de
    put $ deUser  de
    put $ deGroup de
    put $ deMode  de
    put $ deType  de
  get = DirEnt <$> get <*> get <*> get <*> get <*> get <*> get

instance Serialize FileType where
  put RegularFile = putWord8 0x0
  put Directory   = putWord8 0x1
  put Symlink     = putWord8 0x2
  --
  get =
    getWord8 >>= \x -> case x of
      0x0 -> return RegularFile
      0x1 -> return Directory
      0x2 -> return Symlink
      _   -> fail "Invalid FileType during deserialize"

instance Serialize FileMode where
  put FileMode{ fmOwnerPerms = op, fmGroupPerms = gp, fmUserPerms = up } = do
    when (any (>3) $ map length [op, gp, up]) $
      fail "Fixed-length check failed in FileMode serialization"
    putWord8 $ perms op
    putWord8 $ perms gp
    putWord8 $ perms up
    where
      perms ps  = foldr (.|.) 0x0 $ flip map ps $ \x -> -- toBit
                  case x of Read -> 4 ; Write -> 2; Execute -> 1
  --
  get = 
    FileMode <$> gp <*> gp <*> gp 
    where
      gp         = fromBits `fmap` getWord8
      fromBits x = let x0 = if testBit x 0 then [Execute] else []
                       x1 = if testBit x 1 then Write:x0  else x0
                       x2 = if testBit x 2 then Read:x1   else x1
                   in x2

instance Eq FileMode where
  fm1 == fm2 =
    sort (fmOwnerPerms fm1) == sort (fmOwnerPerms fm2) &&
    sort (fmGroupPerms fm1) == sort (fmGroupPerms fm2) &&
    sort (fmUserPerms  fm1) == sort (fmUserPerms  fm2)



               

