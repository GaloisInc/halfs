{-# LANGUAGE Rank2Types, FlexibleContexts #-}

module Tests.Utils
where

import Data.Word
import Control.Monad.ST
import Foreign.C.Error
import System.Directory
import System.IO
import System.IO.Unsafe (unsafePerformIO)
import System.FilePath
import Test.QuickCheck hiding (numTests)
import Test.QuickCheck.Monadic

import qualified Data.ByteString as BS
import qualified Data.Map as M

import Halfs.BlockMap
import Halfs.Classes
import Halfs.CoreAPI (mount, newfs, unmount)
import Halfs.Directory
import Halfs.Errors
import Halfs.HalfsState
import Halfs.Monad
import Halfs.MonadUtils
import Halfs.Protection
import Halfs.SuperBlock
import Halfs.Utils   (divCeil, withDHLock)

import System.Device.BlockDevice
import System.Device.File
import System.Device.Memory
import System.Device.ST

import Tests.Instances
import Tests.Types

-- import Debug.Trace

type DevCtor          = BDGeom -> IO (Maybe (BlockDevice IO))
type HalfsM b r l m a = HalfsT HalfsError (Maybe (HalfsState b r l m)) m a

--------------------------------------------------------------------------------
-- Utility functions

fileDev :: DevCtor
fileDev g = withFileStore
              True
              ("./pseudo.dsk")
              (bdgSecSz g)
              (bdgSecCnt g)
              (`newFileBlockDevice` (bdgSecSz g))

memDev :: DevCtor
memDev g = newMemoryBlockDevice (bdgSecCnt g) (bdgSecSz g)

-- | Create an STArray-backed block device.  This function transforms
-- the ST-based block device to an IO block device for interface
-- consistency within this module.
staDev :: DevCtor
staDev g =
  stToIO (newSTBlockDevice (bdgSecCnt g) (bdgSecSz g)) >>=
  return . maybe Nothing (\dev ->
    Just BlockDevice {
        bdBlockSize = bdBlockSize dev
      , bdNumBlocks = bdNumBlocks dev
      , bdReadBlock  = \i   -> stToIO $ bdReadBlock dev i
      , bdWriteBlock = \i v -> stToIO $ bdWriteBlock dev i v
      , bdFlush      = stToIO $ bdFlush dev
      , bdShutdown   = stToIO $ bdShutdown dev
    })

rescaledDev :: BDGeom  -- ^ geometry for underlying device
            -> BDGeom  -- ^ new device geometry
            -> DevCtor -- ^ ctor for underlying device
            -> IO (Maybe (BlockDevice IO))
rescaledDev oldG newG ctor =
  maybe (fail "Invalid BlockDevice") (newRescaledBlockDevice (bdgSecSz newG))
    `fmap` ctor oldG

monadicBCMIOProp :: PropertyM (BCM IO) a -> Property
monadicBCMIOProp = monadic (unsafePerformIO . runBCM)

withFileStore :: Bool -> FilePath -> Word64 -> Word64 -> (FilePath -> IO a)
              -> IO a
withFileStore temp fp secSize secCnt act = do
  (fname, h) <-
    if temp
     then openBinaryTempFile
            (let d = takeDirectory "." in if null d then "." else d)
            (takeFileName fp)
     else (,) fp `fmap` openBinaryFile fp ReadWriteMode

  let chunkSz               = 2^(20::Int)
      (numChunks, numBytes) = fromIntegral (secSize * secCnt) `divMod` chunkSz
      chunk = BS.replicate chunkSz 0
  replicateM_ numChunks (BS.hPut h chunk)
  BS.hPut h (BS.replicate numBytes 0)
  hClose h
  rslt <- act fname
  when temp $ removeFile fname
  return rslt

whenDev :: (Monad m) => (a -> m b) -> (a -> m ()) -> Maybe a -> m b
whenDev act cleanup =
  maybe (fail "Invalid BlockDevice") $ \x -> do
    y <- act x
    cleanup x
    return y

mkMemDevExec :: forall m.
                Bool
             -> String
             -> Int
             -> String
             -> (BDGeom -> BlockDevice IO -> PropertyM IO m)
             -> (Args, Property)
mkMemDevExec quick pfx =
  let numTests n = (,) $ if quick then stdArgs{maxSuccess = n} else stdArgs
      doProp     = (`whenDev` run . bdShutdown)
  in
    \n s pr ->
      numTests n $ label (pfx ++ ": " ++ s) $ monadicIO $
        forAllM arbBDGeom $ \g ->
          run (memDev g) >>= doProp (pr g)

mkNewFS :: HalfsCapable b t r l m =>
           BlockDevice m -> PropertyM m (Either HalfsError SuperBlock)
mkNewFS dev = runHNoEnv $ newfs dev rootUser rootGroup rootDirPerms

mountOK :: HalfsCapable b t r l m =>
           BlockDevice m
        -> PropertyM m (HalfsState b r l m)
mountOK dev = do
  runHNoEnv (defaultMount dev)
    >>= either (fail . (++) "Unexpected mount failure: " . show) return

unmountOK :: HalfsCapable b t r l m =>
             HalfsState b r l m -> PropertyM m ()
unmountOK fs =
  runH fs unmount >>=
    either (fail . (++) "Unexpected unmount failure: " . show)
           (const $ return ())

sreadRef :: HalfsCapable b t r l m => r a -> PropertyM m a
sreadRef = ($!) (run . readRef)

runH :: HalfsCapable b t r l m =>
        HalfsState b r l m
     -> HalfsM b r l m a
     -> PropertyM m (Either HalfsError a)
runH fs = run . runHalfs fs

runHNoEnv :: HalfsCapable b t r l m =>
             HalfsM b r l m a
          -> PropertyM m (Either HalfsError a)
runHNoEnv = run . runHalfsNoEnv

execE :: (Monad m ,Show a) =>
         String -> String -> m (Either a b) -> PropertyM m b
execE nm descrip act =
  run act >>= \ea -> case ea of
    Left e  ->
      fail $ "Unexpected error in " ++ nm ++ " ("
           ++ descrip ++ "): " ++ show e
    Right x -> return x

execH :: Monad m =>
         String
      -> env
      -> String
      -> HalfsT HalfsError (Maybe env) m b
      -> PropertyM m b
execH nm env descrip = execE nm descrip . runHalfs env

execHNoEnv :: Monad m =>
              String
           -> String
           -> HalfsT HalfsError (Maybe env) m b
           -> PropertyM m b
execHNoEnv nm descrip = execE nm descrip . runHalfsNoEnv

expectErr :: HalfsCapable b t r l m =>
             (HalfsError -> Bool)
          -> String
          -> HalfsM b r l m a
          -> HalfsState b r l m
          -> PropertyM m ()
expectErr expectedP rsn act fs =
  runH fs act >>= \e -> case e of
    Left err | expectedP err -> return ()
    Left err                 -> unexpectedErr err
    Right _                  -> fail rsn

unexpectedErr :: (Monad m, Show a) => a -> PropertyM m ()
unexpectedErr = fail . (++) "Expected failure, but not: " . show

expectErrno :: Monad m => Errno -> Either HalfsError a -> PropertyM m ()
expectErrno e (Left (HE_ErrnoAnnotated _ errno)) = assert (errno == e)
expectErrno _ _                                  = assert False

checkFileStat :: (HalfsCapable b t r l m, Integral a) =>
                 FileStat t
              -> a           -- expected filesize
              -> FileType    -- expected filetype
              -> FileMode    -- expected filemode
              -> UserID      -- expected userid
              -> GroupID     -- expected groupid
              -> a           -- expected allocated block count
              -> (t -> Bool) -- access time predicate
              -> (t -> Bool) -- modification time predicate
              -> (t -> Bool) -- status change time predicate
              -> PropertyM m ()
checkFileStat st expFileSz expFileTy expMode
              expUsr expGrp expNumBlocks accessp modifyp changep = do
  mapM_ assert
    [ fsSize      st == fromIntegral expFileSz
    , fsType      st == expFileTy
    , fsMode      st == expMode
    , fsUID       st == expUsr
    , fsGID       st == expGrp
    , fsNumBlocks st == fromIntegral expNumBlocks
    , accessp (fsAccessTime st)
    , modifyp (fsModifyTime st)
    , changep (fsChangeTime st)
    ]

assertMsg :: Monad m => String -> String -> Bool -> PropertyM m ()
assertMsg _ _ True       = return ()
assertMsg ctx dtls False = do
  fail $ "(" ++ ctx ++ ": " ++ dtls ++ ")"

-- Using the current allocation scheme and inode/cont distinction,
-- determine how many blocks (of the given size, in bytes) are required
-- to store the given data size, in bytes.
calcExpBlockCount :: Integral a =>
                     Word64 -- block size
                  -> Word64 -- addresses (#blocks) per inode
                  -> Word64 -- addresses (#blocks) per cont
                  -> a      -- data size
                  -> a      -- expected number of blocks
calcExpBlockCount bs api apc dataSz = fromIntegral $
  if dsz > bpi
  then 1                           -- inode block
       + api                       -- number of blocks in full inode
       + (dsz - bpi) `divCeil` bpc -- number of blocks required for conts
       + (dsz - bpi) `divCeil` bs  -- number of blocks rquired for data
  else 1                           -- inode block
       + (dsz `divCeil` bs)        -- number of blocks required for data
  where
    dsz = fromIntegral dataSz
    bpi = api * bs
    bpc = apc * bs

defaultUser :: UserID
defaultUser = rootUser

defaultGroup :: GroupID
defaultGroup = rootGroup

rootDirPerms, defaultDirPerms, defaultFilePerms :: FileMode
rootDirPerms     = FileMode [Read,Write,Execute] [] []
defaultDirPerms  = FileMode [Read,Write,Execute] [Read, Execute] [Read, Execute]
defaultFilePerms = FileMode [Read,Write] [Read] [Read]

defaultMount :: HalfsCapable b t r l m =>
                BlockDevice m -> HalfsM b r l m (HalfsState b r l m)
defaultMount dev = mount dev defaultUser defaultGroup defaultDirPerms

--------------------------------------------------------------------------------
-- Block utilization checking combinators

rscUtil :: HalfsCapable b t r l m =>
           (Word64 -> Word64 -> Bool) -- ^ predicate on after/before block cnts
        -> HalfsState b r l m         -- ^ the filesystem state
        -> PropertyM m a              -- ^ the action to check
        -> PropertyM m ()
rscUtil p fs act = do b <- getFree fs; _ <- act; a <- getFree fs; assert (p a b)
                   where getFree = sreadRef . bmNumFree . hsBlockMap

blocksUnallocd :: HalfsCapable b t r l m =>
                  Word64             -- ^ expected #blocks unallocated
               -> HalfsState b r l m -- ^ the filesystem state
               -> PropertyM m a      -- ^ the action to check
               -> PropertyM m ()
blocksUnallocd x = rscUtil (\a b -> a >= b && a - b == x)

blocksAllocd :: HalfsCapable b t r l m =>
                Word64             -- ^ expected #blocks unallocated
             -> HalfsState b r l m -- ^ the filesystem state
             -> PropertyM m a      -- ^ the action to check
             -> PropertyM m ()
blocksAllocd x = rscUtil (\a b -> b >= a && b - a == x)

zeroOrMoreBlocksAllocd :: HalfsCapable b t r l m =>
                          HalfsState b r l m -- ^ the filesystem state
                       -> PropertyM m a      -- ^ the action to check
                       -> PropertyM m ()
zeroOrMoreBlocksAllocd = rscUtil (<=)


--------------------------------------------------------------------------------
-- Debugging helpers

dumpfs :: HalfsCapable b t r l m =>
          HalfsM b r l m String
dumpfs = do
  sbRef <- hasks hsSuperBlock
  dump <- dumpfs' 2 "/\n" =<< rootDir `fmap` readRef sbRef
  return $ "=== fs dump begin ===\n"
        ++ dump
        ++ "=== fs dump end ===\n"
  where
    dumpfs' i ipfx inr = do
      contents <- withDirectory inr $ \dh -> do
                    withDHLock dh $ readRef (dhContents dh)
      foldM (\dumpAcc (path, dirEnt) -> do
               sub <- if deType dirEnt == Directory
                         && path /= "."
                         && path /= ".."
                        then dumpfs' (i+2) "" (deInode dirEnt)
                        else return ""
               return $ dumpAcc
                     ++ replicate i ' '
                     ++ path
                     ++ let inr' = deInode dirEnt in
                        case deType dirEnt of
                          RegularFile -> " (" ++ show inr' ++ ") (file)\n"
                          Directory   -> " (" ++ show inr' ++ ") (directory)\n" ++ sub
                          Symlink     -> " (" ++ show inr' ++ ") (symlink)\n"
                          _           -> error "unknown file type"
            )
            ipfx (M.toList contents)
