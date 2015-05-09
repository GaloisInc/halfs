module Main where

import Control.Exception
import qualified Data.ByteString.Char8 as B
import Foreign.C.Error
import System.Directory ( getDirectoryContents )
import System.IO
import System.IO.Error
import System.Posix
import System.Fuse

type HT = Fd

main :: IO ()
main = fuseMain bindFSOps (\e -> print e >> bindExceptionHandler e)

bindExceptionHandler :: Exception -> IO Errno
bindExceptionHandler (IOException ioe)
    | isAlreadyExistsError ioe = return eALREADY
    | isDoesNotExistError  ioe = return eNOENT
    | isAlreadyInUseError  ioe = return eBUSY
    | isFullError          ioe = return eAGAIN
    | isEOFError           ioe = return eIO
    | isIllegalOperation   ioe = return eNOTTY
    | isPermissionError    ioe = return ePERM
    | otherwise                = return eFAULT
bindExceptionHandler e         = return eFAULT

bindFSOps :: FuseOperations HT
bindFSOps =
    defaultFuseOps { fuseGetFileStat = bindGetFileStat
                   , fuseReadSymbolicLink = bindReadSymbolicLink
                   , fuseCreateDevice = bindCreateDevice
                   , fuseCreateDirectory = bindCreateDirectory
                   , fuseRemoveLink = bindRemoveLink
                   , fuseRemoveDirectory = bindRemoveDirectory
                   , fuseCreateSymbolicLink = bindCreateSymbolicLink
                   , fuseRename = bindRename
                   , fuseCreateLink = bindCreateLink
                   , fuseSetFileMode = bindSetFileMode
                   , fuseSetOwnerAndGroup = bindSetOwnerAndGroup
                   , fuseSetFileSize = bindSetFileSize
                   , fuseSetFileTimes = bindSetFileTimes
                   , fuseOpen = bindOpen
                   , fuseRead = bindRead
                   , fuseWrite = bindWrite
                   , fuseGetFileSystemStats = bindGetFileSystemStats
                   , fuseFlush = bindFlush
                   , fuseRelease = bindRelease
                   , fuseSynchronizeFile = bindSynchronizeFile
                   , fuseOpenDirectory = bindOpenDirectory
                   , fuseReadDirectory = bindReadDirectory
                   }

fileStatusToEntryType :: FileStatus -> EntryType
fileStatusToEntryType status 
    | isSymbolicLink    status = SymbolicLink
    | isNamedPipe       status = NamedPipe
    | isCharacterDevice status = CharacterSpecial
    | isDirectory       status = Directory
    | isBlockDevice     status = BlockSpecial
    | isRegularFile     status = RegularFile
    | isSocket          status = Socket
    | otherwise                = Unknown

fileStatusToFileStat :: FileStatus -> FileStat
fileStatusToFileStat status =
    FileStat { statEntryType        = fileStatusToEntryType status
             , statFileMode         = fileMode status
             , statLinkCount        = linkCount status
             , statFileOwner        = fileOwner status
             , statFileGroup        = fileGroup status
             , statSpecialDeviceID  = specialDeviceID status
             , statFileSize         = fileSize status
             -- fixme: 1024 is not always the size of a block
             , statBlocks           = fromIntegral (fileSize status `div` 1024)
             , statAccessTime       = accessTime status
             , statModificationTime = modificationTime status
             , statStatusChangeTime = statusChangeTime status
             }

bindGetFileStat :: FilePath -> IO (Either Errno FileStat)
bindGetFileStat path =
    do status <- getSymbolicLinkStatus path
       return $ Right $ fileStatusToFileStat status

bindReadSymbolicLink :: FilePath -> IO (Either Errno FilePath)
bindReadSymbolicLink path =
    do target <- readSymbolicLink path
       return (Right target)

bindOpenDirectory :: FilePath -> IO Errno
bindOpenDirectory path =
    do openDirStream path >>= closeDirStream
       return eOK

bindReadDirectory :: FilePath -> IO (Either Errno [(FilePath, FileStat)])
bindReadDirectory path =
    do names <- getDirectoryContents path
       mapM pairType names >>= return . Right
    where pairType name =
              do status <- getSymbolicLinkStatus (path ++ "/" ++ name)
                 return (name, fileStatusToFileStat status)

bindCreateDevice :: FilePath -> EntryType -> FileMode -> DeviceID -> IO Errno
bindCreateDevice path entryType mode dev =
    do let combinedMode = entryTypeToFileMode entryType `unionFileModes` mode
       createDevice path combinedMode dev
       return eOK

bindCreateDirectory :: FilePath -> FileMode -> IO Errno
bindCreateDirectory path mode =
    do createDirectory path mode
       return eOK

bindRemoveLink :: FilePath -> IO Errno
bindRemoveLink path =
    do removeLink path
       return eOK

bindRemoveDirectory :: FilePath -> IO Errno
bindRemoveDirectory path =
    do removeDirectory path
       return eOK

bindCreateSymbolicLink :: FilePath -> FilePath -> IO Errno
bindCreateSymbolicLink src dest =
    do createSymbolicLink src dest
       return eOK

bindRename :: FilePath -> FilePath -> IO Errno
bindRename src dest =
    do rename src dest
       return eOK

bindCreateLink :: FilePath -> FilePath -> IO Errno
bindCreateLink src dest =
    do createLink src dest
       return eOK

bindSetFileMode :: FilePath -> FileMode -> IO Errno
bindSetFileMode path mode =
    do setFileMode path mode
       return eOK

bindSetOwnerAndGroup :: FilePath -> UserID -> GroupID -> IO Errno
bindSetOwnerAndGroup path uid gid =
    do setOwnerAndGroup path uid gid
       return eOK

bindSetFileSize :: FilePath -> FileOffset -> IO Errno
bindSetFileSize path off =
    do setFileSize path off
       return eOK

bindSetFileTimes :: FilePath -> EpochTime -> EpochTime -> IO Errno
bindSetFileTimes path accessTime modificationTime =
    do setFileTimes path accessTime modificationTime
       return eOK

bindOpen :: FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno HT)
bindOpen path mode flags =
    do fd <- openFd path mode Nothing flags
       return (Right fd)

bindRead :: FilePath -> HT -> ByteCount -> FileOffset -> IO (Either Errno B.ByteString)
bindRead path fd count off =
    do newOff <- fdSeek fd AbsoluteSeek off
       if off /= newOff
          then do return (Left eINVAL)
          else do (content, bytesRead) <- fdRead fd count 
                  return (Right $ B.pack content)

bindWrite :: FilePath -> HT -> B.ByteString -> FileOffset -> IO (Either Errno ByteCount)
bindWrite path fd buf off =
    do newOff <- fdSeek fd AbsoluteSeek off
       if off /= newOff
          then do return (Left eINVAL)
          else do res <- fdWrite fd $ B.unpack buf
                  return (Right res)

bindGetFileSystemStats :: String -> IO (Either Errno FileSystemStats)
bindGetFileSystemStats _ = return (Left eOK)

bindFlush :: FilePath -> HT -> IO Errno
bindFlush _ _ = return eOK

bindRelease :: FilePath -> HT -> IO ()
bindRelease _ fd = closeFd fd

bindSynchronizeFile :: FilePath -> SyncType -> IO Errno
bindSynchronizeFile _ _ = return eOK
