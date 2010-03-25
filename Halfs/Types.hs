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

newtype Ref a = Ref Word64 
  deriving (Eq, Ord, Num, Show, Integral, Enum, Real)

instance Serialize (Ref a) where
  put (Ref x) = putWord64be x
  get         = Ref `fmap` getWord64be

-- We store Inode/Cont references as simple Word64s, newtype'd in case
-- we either decide to do something more fancy or just to make the types
-- a bit more clear.
--
-- At this point, we assume a reference is equal to its block address,
-- and we fix references as Word64s. Note that if you change the
-- underlying field size of an InodeRef/ContRef, you *really* (!)  need
-- to change 'refSize', below.
--

newtype InodeRef = IR { unIR :: Word64 }
  deriving (Eq, Ord, Num, Show, Integral, Enum, Real)

newtype ContRef = CR { unCR :: Word64 }
  deriving (Eq, Show)

instance Serialize InodeRef where
  put (IR x) = putWord64be x
  get        = IR `fmap` getWord64be

instance Serialize ContRef where
  put (CR x) = putWord64be x
  get        = CR `fmap` getWord64be

-- | The size of an Inode/Cont reference in bytes
refSize :: Word64
refSize = 8

-- | A simple locked resource reference
data LockedRscRef l r rsc = LockedRscRef
  { lrLock :: l
  , lrRsc  :: r rsc
  }

--------------------------------------------------------------------------------
-- Common Directory and File Types

-- File names are arbitrary-length, null-terminated strings.  Valid file names
-- are guaranteed to not include null or the System.FilePath.pathSeparator
-- character.

-- Current directory and parent directory relative path names
dotPath, dotdotPath :: FilePath
dotPath    = "."
dotdotPath = ".."

-- | DF_WrongFileType implies the filesystem element with the search key
-- was found but was not of the correct type.
data DirFindRslt a = DF_NotFound | DF_WrongFileType FileType | DF_Found a

data DirectoryEntry = DirEnt
  { deName  :: String
  , deInode :: InodeRef
  , deUser  :: UserID
  , deGroup :: GroupID
  , deMode  :: FileMode
  , deType  :: FileType
  }
  deriving (Show, Eq)

data DirHandle r l = DirHandle
  { dhInode       :: InodeRef
  , dhContents    :: r (M.Map FilePath DirectoryEntry)
  , dhState       :: r DirectoryState
  , dhLock        :: l
  }

data AccessRight = Read | Write | Execute
  deriving (Show, Eq, Ord)

-- Isomorphic to System.Posix.IO.OpenMode, but present here to avoid explicit
-- dependency on the Posix module(s).
data FileOpenMode = ReadOnly | WriteOnly | ReadWrite
  deriving (Eq, Show)

-- Similar to System.Posix.IO.OpenFileFlags, but present here to avoid explicit
-- dependency on the Posix module(s).
data FileOpenFlags = FileOpenFlags
  { append   :: Bool         -- append on each write
  , nonBlock :: Bool         -- do not block on open or for data to become avail

{- Always False from HFuse 0.2.2!
  , explicit :: Bool         -- atomically obtain an exclusive lock
  , truncate :: Bool         -- truncate size to 0
-}

-- Not yet supported by halfs
-- , noctty :: Bool

  , openMode :: FileOpenMode -- isomorphic to System.Posix.IO.OpenMode
  }
  deriving (Show)

data DirectoryState = Clean | OnlyAdded | OnlyDeleted | VeryDirty
  deriving (Show, Eq)

data FileMode = FileMode
  { fmOwnerPerms :: [AccessRight]
  , fmGroupPerms :: [AccessRight]
  , fmUserPerms  :: [AccessRight]
  }
  deriving (Show)

data FileType = RegularFile | Directory | Symlink | AnyFileType
  deriving (Show, Eq)

data Show t => FileStat t = FileStat
  { fsInode      :: InodeRef
  , fsType       :: FileType
  , fsMode       :: FileMode 
  , fsNumLinks   :: Word64   -- ^ Number of hardlinks to the file
  , fsUID        :: UserID
  , fsGID        :: GroupID
  , fsSize       :: Word64   -- ^ File size, in bytes
  , fsNumBlocks  :: Word64   -- ^ Number of blocks allocated
  , fsAccessTime :: t        -- ^ Time of last access
  , fsModTime    :: t        -- ^ Time of last data modification
  , fsChangeTime :: t        -- ^ Time of last status (inode) change
  }
  deriving Show


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
  put _           = fail "Invalid FileType during serialize"
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

instance Functor DirFindRslt where
  fmap _ DF_NotFound           = DF_NotFound
  fmap _ (DF_WrongFileType ft) = DF_WrongFileType ft
  fmap f (DF_Found r)          = DF_Found (f r)
