{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Snap.Snaplet.Storage where
import Control.Applicative
import Control.Lens

import qualified Data.ByteString.Char8 as B
import           Data.IORef
import qualified Data.Map.Strict       as M
import           Data.Time

import qualified Data.Foldable    as F
import qualified Data.Traversable as F

import System.Directory
import System.Directory.Tree

data Store a = Store
  { rootDir      :: !FilePath
  , loadObject   :: !(FilePath -> IO a)
  , storeObjects :: !(IORef (M.Map B.ByteString (Dated a)))
  }

data Dated a = Dated { _date :: !UTCTime, _object :: !a }
  deriving (Show, Eq)

makeLenses ''Dated

foldTree :: AnchoredDirTree v -> M.Map B.ByteString v
foldTree = F.foldl' (\h (k,v) -> M.insert (B.pack k) v h) M.empty . zipPaths

objectTree
  :: (FilePath -> IO a)
  -> FilePath
  -> IO (M.Map B.ByteString (Dated a))
objectTree loadObj root = foldTree <$> readDirectoryWith loadDatedObj root
 where
  loadDatedObj path = Dated <$> getModificationTime path <*> loadObj path

data Load a
  = Reload !FilePath
  | Keep !a
 deriving (Show, Eq, Ord)

findUpdates
  :: M.Map B.ByteString (Dated a)
  -> M.Map B.ByteString (Dated FilePath)
  -> M.Map B.ByteString (Dated (Load a))
findUpdates = M.mergeWithKey
  (\_ (Dated t0 o0) (Dated t1 path) -> Just $!
     if t1 > t0
     then Dated t1 (Reload path)
     else Dated t0 (Keep o0))
  (traverse.object %~ Keep)
  (traverse.object %~ Reload)

loadUpdates
  :: Applicative f
  => (FilePath -> f a)
  -> M.Map B.ByteString (Dated (Load a))
  -> f (M.Map B.ByteString (Dated a))
loadUpdates load = F.traverse $ \(Dated t a) -> do
  case a of
    Reload k -> Dated t <$> load k
    Keep   x -> pure $! Dated t x

updateStore :: Store a -> IO ()
updateStore store = do
  oldTree <- readIORef (storeObjects store)
  newTree <- objectTree return (rootDir store)
  objects <- loadUpdates (loadObject store) $! findUpdates oldTree newTree
  writeIORef (storeObjects store) objects

newStore :: FilePath -> (FilePath -> IO a) -> IO (Store a)
newStore root load = do
  objects <- newIORef M.empty
  let store = Store root load objects
  updateStore store
  return store
