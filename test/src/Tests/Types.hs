module Tests.Types
where

import Data.Word

data BDGeom = BDGeom
  { bdgSecCnt :: Word64       -- ^ number of sectors
  , bdgSecSz  :: Word64       -- ^ sector size, in bytes
  } deriving Show

