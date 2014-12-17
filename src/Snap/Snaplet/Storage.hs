{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE FunctionalDependencies    #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Snap.Snaplet.Storage where
import           Control.Applicative
import           Control.Concurrent
import           Control.Lens
import qualified Data.ByteString.Char8 as B
import qualified Data.Foldable         as F
import qualified Data.HashMap.Strict   as H
import           Data.IORef
import qualified Data.Set              as Set
import           Data.Time
import qualified Data.Traversable      as F
import           Snap
import           System.Directory
import           System.Directory.Tree

data Store a = Store
  { storeObjects    :: !(IORef (H.HashMap B.ByteString (Dated a)))
  , _scurrentObject :: Maybe (Dated a)
  }

newtype ModTimes = ModTimes (Set.Set UTCTime)
  deriving (Eq, Show)

instance Ord ModTimes where
  compare (ModTimes a) (ModTimes b)
    | Set.null a && Set.null b = EQ
    | Set.null a               = LT
    | Set.null b               = GT
    | otherwise                =
        let (a', arem) = Set.deleteFindMax a
            (b', brem) = Set.deleteFindMax b
        in case compare a' b' of
             EQ   -> compare arem brem
             ltGt -> ltGt

mergeModTimes :: ModTimes -> ModTimes -> ModTimes
mergeModTimes (ModTimes a) (ModTimes b) = ModTimes (Set.union a b)

data Dated a = Dated { _ddate :: !ModTimes, _dobject :: !a }
  deriving (Show, Eq)

makeLenses ''Store
makeLenses ''Dated

--------------------------------------------------------------------------------
-- Main logic

foldTree :: AnchoredDirTree v -> H.HashMap B.ByteString v
foldTree = F.foldl' (\h (k,v) -> H.insert (B.pack k) v h) H.empty . zipPaths

objectTree
  :: (FilePath -> IO a)
  -> FilePath
  -> IO (H.HashMap B.ByteString (Dated a))
objectTree loadObj root = foldTree <$> readDirectoryWith loadDatedObj root
 where
  getModTime   p = ModTimes . Set.singleton <$> getModificationTime p
  loadDatedObj p = Dated <$> getModTime p <*> loadObj p

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
       then Dated (mergeModTimes t0 t1) o1
       else Dated t0 o0)
    -- remove entries not in the new tree, and make them both the same type.
    (traverse.dobject %~ Keep $ H.intersection m0 m1)
    (traverse.dobject %~ Load $ m1)

loadUpdates
  :: forall f a. (Applicative f, Monad f)
  => (FilePath -> f (Maybe a))
  -> H.HashMap B.ByteString (Dated (Load a))
  -> f (H.HashMap B.ByteString (Dated a))
loadUpdates load hmap = execStateT (insertEach hmap) H.empty
 where
  insertEach
    :: H.HashMap B.ByteString (Dated (Load a))
    -> StateT (H.HashMap B.ByteString (Dated a)) f ()
  insertEach = itraverse_ $ \key (Dated t a) ->
    case a of
      Load k -> do
        obj <- lift (load k)
        F.for_ obj (modify . H.insert key . Dated t)
      Keep x -> modify (H.insert key (Dated t x))

updateStore :: StoreConfig a -> Store a -> IO ()
updateStore config store = do
  oldTree <- readIORef (storeObjects store)
  newTree <- objectTree return (storeRoot config)
  objects <- loadUpdates (readObject config) $! findUpdates oldTree newTree
  writeIORef (storeObjects store) objects

newStore :: StoreConfig a -> IO (Store a)
newStore config = do
  objects <- newIORef H.empty
  let store = Store objects Nothing
  updateStore config store
  return store

lookupStore :: B.ByteString -> Store a -> IO (Maybe (Dated a))
lookupStore spath store = H.lookup spath <$> readIORef (storeObjects store)

--------------------------------------------------------------------------------
-- Snap integration

class HasStore b a | b -> a where
  storeLens :: SnapletLens (Snaplet b) (Store a)

currentObject :: HasStore b a => Handler b v (Maybe (Dated a))
currentObject = withTop' storeLens (use scurrentObject)

data StoreConfig a = StoreConfig
  { storeRoot  :: !FilePath
  , readObject :: !(FilePath -> IO (Maybe a))
  , pollEvery  :: !DiffTime
  }

storeInit
  :: HasStore b a
  => StoreConfig a
  -> SnapletInit b (Store a)
storeInit config = makeSnaplet "store" "Snap store init" Nothing $ do
  store    <- liftIO (newStore config)
  reloader <- liftIO . forkIO . forever $ do
    threadDelay $! floor (1000000 * toRational (pollEvery config))
    updateStore config store

  onUnload (killThread reloader)

  addRoutes
    [("", do
      spath  <- getsRequest rqPathInfo
      object <- liftIO (lookupStore spath store)
      scurrentObject .= object
      pass)
    ]

  return store

