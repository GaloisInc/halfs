module Main where

import Control.Concurrent.MVar

import HFuse

type FileName = String

type Permissions = (FileMode, UserID, GroupID)

type FSMap = FiniteMap FilePath FSObject

data FSObject
    = FSRegularFile Permissions String
    | FSDirectory Permissions [(FileName, FSObject)]
 
fsObjectPersmissions :: FSObject -> Persmissions
fsObjectPersmissions (FSRegularFile perms _) = perms
fsObjectPersmissions (FSDirectory   perms _) = perms

main :: IO ()
main =
    do mvRoot <- newMVar $ Directory []
       let liveFSOps :: FuseOperations
           liveFSOps =
               FuseOperations { fuseGetFileStat = liveGetFileStat
                              , fuseReadSymbolicLink = liveReadSymbolicLink
                              , fuseGetDirectoryContents = liveGetDirectoryContents
                              , fuseCreateDevice = liveCreateDevice
                              , fuseCreateDirectory = liveCreateDirectory
                              , fuseRemoveLink = liveRemoveLink
                              , fuseRemoveDirectory = liveRemoveDirectory
                              , fuseCreateSymbolicLink = liveCreateSymbolicLink
                              , fuseRename = liveRename
                              , fuseCreateLink = liveCreateLink
                              , fuseSetFileMode = liveSetFileMode
                              , fuseSetOwnerAndGroup = liveSetOwnerAndGroup
                              , fuseSetFileSize = liveSetFileSize
                              , fuseSetFileTimes = liveSetFileTimes
                              , fuseOpen = liveOpen
                              , fuseRead = liveRead
                              , fuseWrite = liveWrite
                              , fuseGetFileSystemStats = liveGetFileSystemStats
                              , fuseFlush = liveFlush
                              , fuseRelease = liveRelease
                              , fuseSynchronizeFile = liveSynchronizeFile
                              }
           withPath :: FilePath -> (Maybe FSObject -> a) -> IO a
           withPath path f =
               withMVar mvRoot $ \ root ->
                   f (foldl digPath root (paths path))
           digPath :: Maybe FSObject -> FileName -> Maybe FSObject
           digPath (Just (Directory entries)) name = lookup name entries
           digPath _                          _    = Nothing
           liveGetFileStat :: FilePath -> IO (Either Errno FileStat)
           liveGetFileStat path =
               withPath path $ \ mbObj -> case mbObj of
                 Nothing  -> return (Left eNOENT)
                 Just obj -> let entryType = case obj of FSRegularFile _ -> RegularFile
                                                         Directory _ -> Directory
                                 (mode, owner, group) = fsObjectPermissions obj
                                 size = 
                             in return $ Right $ FileStat
                                    { statEntryType = entryType
                                    , statFileMode  = mode
                                    , statLinkCount = 1
                                    , statFileOwner = owner
                                    , statFileGroup = group
                                    , statSpecialDeviceID = 0
                                    , statFileSize  = 
       fuseMain liveFSOps (\e -> print e >> defaultExceptionHandler e)

paths :: FilePath -> [FileName]
paths s = case dropWhile (== '/') s of
            "" -> []
            s' -> w : words s''
    where (w, s'') = break (== '/') s'
