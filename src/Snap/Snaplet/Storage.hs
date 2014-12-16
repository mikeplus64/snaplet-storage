{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Snap.Snaplet.Storage where
import Control.Applicative
import Control.Lens

import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict   as H
import           Data.IORef
import           Data.Time

import qualified Data.Foldable    as F
import qualified Data.Traversable as F

import System.Directory
import System.Directory.Tree

import Debug.Trace

data Store a = Store
  { rootDir      :: !FilePath
  , loadObject   :: !(FilePath -> IO a)
  , storeObjects :: !(IORef (H.HashMap B.ByteString (Dated a)))
  }

data Dated a = Dated { _date :: !UTCTime, _object :: !a }
  deriving (Show, Eq)

makeLenses ''Dated

foldTree :: AnchoredDirTree v -> H.HashMap B.ByteString v
foldTree = F.foldl' (\h (k,v) -> H.insert (B.pack k) v h) H.empty . zipPaths

objectTree
  :: (FilePath -> IO a)
  -> FilePath
  -> IO (H.HashMap B.ByteString (Dated a))
objectTree loadObj root = foldTree <$> readDirectoryWith loadDatedObj root
 where
  loadDatedObj path = Dated <$> getModificationTime path <*> loadObj path

data Load a
  = Load !FilePath
  | Keep !a
 deriving (Show, Eq, Ord)

findUpdates
  :: H.HashMap B.ByteString (Dated a)
  -> H.HashMap B.ByteString (Dated FilePath)
  -> H.HashMap B.ByteString (Dated (Load a))
findUpdates m0 m1 =
  H.unionWith
    (\(Dated t0 o0) (Dated t1 o1) ->
       if t1 > t0
       then Dated t1 o1
       else Dated t0 o0)
    -- remove entries not in the new tree, and make them both the same type.
    (traverse.object %~ Keep $! H.intersection m0 m1)
    (traverse.object %~ Load $! m1)

loadUpdates
  :: Applicative f
  => (FilePath -> f a)
  -> H.HashMap B.ByteString (Dated (Load a))
  -> f (H.HashMap B.ByteString (Dated a))
loadUpdates load = F.traverse $ \(Dated t a) -> do
  case a of
    Load k -> Dated t <$> load k
    Keep x -> pure $! Dated t x

updateStore :: Store a -> IO ()
updateStore store = do
  oldTree <- readIORef (storeObjects store)
  newTree <- objectTree return (rootDir store)
  objects <- loadUpdates (loadObject store) $! findUpdates oldTree newTree
  writeIORef (storeObjects store) objects

newStore :: FilePath -> (FilePath -> IO a) -> IO (Store a)
newStore root load = do
  objects <- newIORef H.empty
  let
    load' = \p -> print p >> load p
    store = Store root load' objects
  updateStore store
  return store

lookupStore :: B.ByteString -> Store a -> IO (Maybe (Dated a))
lookupStore path store = H.lookup path <$> readIORef (storeObjects store)
