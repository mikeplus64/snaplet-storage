{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE FunctionalDependencies    #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Snap.Snaplet.Storage
  ( HasStore(..)
  , StoreConfig(..)
  , htmlStore
    -- * Types
  , Store
  , Dated(..), ModTimes, times
    -- * Snap integratoin
  , storeInit
  , storeUse
  , storeGet
  , storeObject
    -- * IO functions
  , newStore, updateStore, lookupStore
  ) where
import           Control.Applicative
import           Control.Concurrent
import           Control.Lens
import qualified Data.ByteString       as B
import qualified Data.Foldable         as F
import qualified Data.HashMap.Strict   as H
import           Data.IORef
import           Data.List             (isSuffixOf)
import qualified Data.Set              as Set
import           Data.String
import           Data.Time
import           Snap
import           System.Directory
import           System.Directory.Tree
import qualified System.IO             as IO

data Store a = Store
  { storeObjects :: !(IORef (H.HashMap B.ByteString (Dated a)))
  }

-- | Instead of storing just the latest time for when something was updated
-- , we store every time that a store object was modified.
newtype ModTimes = ModTimes { _allModTimes :: Set.Set UTCTime }
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

data Dated a = Dated { date :: !ModTimes, object :: !a }
  deriving (Show, Eq)

makeLensesFor
  [("object", "_object")
  ,("date", "_date")
  ] ''Dated

makeLenses ''ModTimes

-- | A lens from the dates of a 'Dated' object to a (descending) list of
-- modification times.
times :: Lens' (Dated a) [UTCTime]
times = _date . allModTimes . iso Set.toDescList Set.fromList

--------------------------------------------------------------------------------
-- Main logic

foldTree :: AnchoredDirTree v -> H.HashMap B.ByteString v
foldTree = F.foldl' (\h (k,v) -> H.insert (fromString k) v h) H.empty . zipPaths

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
    (traverse._object %~ Keep $ H.intersection m0 m1)
    (traverse._object %~ Load $ m1)

data Update a = Update
  { _loaded  :: !Int
  , _kept    :: !Int
  , _rebuilt :: !(H.HashMap B.ByteString (Dated a))
  }

makeLenses ''Update

loadUpdates
  :: forall f a. (Applicative f, Monad f)
  => (FilePath -> f (Maybe a))
  -> H.HashMap B.ByteString (Dated (Load a))
  -> f (Update a)
loadUpdates load hmap = execStateT (insertEach hmap) (Update 0 0 H.empty)
 where
  insertEach :: H.HashMap B.ByteString (Dated (Load a))
             -> StateT (Update a) f ()
  insertEach = itraverse_ $ \key (Dated t a) ->
    case a of
      Load k -> do
        mobj <- lift (load k)
        case mobj of
          Just obj -> do
            loaded  += 1
            rebuilt %= H.insert key (Dated t obj)
          Nothing  -> return ()
      Keep x -> do
        kept    += 1
        rebuilt %= H.insert key (Dated t x)

updateStore :: StoreConfig a -> Store a -> IO (Int, Int)
updateStore config store = do
  oldTree <- readIORef (storeObjects store)
  newTree <- objectTree return (storeRoot config)
  update  <- loadUpdates (readObject config) $! findUpdates oldTree newTree
  writeIORef (storeObjects store) (_rebuilt update)
  return (_loaded update, _kept update)

newStore :: StoreConfig a -> IO (Store a)
newStore config = do
  objects <- newIORef H.empty
  let store = Store objects
  updated <- updateStore config store
  print updated
  return store

lookupStore :: B.ByteString -> Store a -> IO (Maybe (Dated a))
lookupStore spath store = H.lookup spath <$> readIORef (storeObjects store)

--------------------------------------------------------------------------------
-- Snap integration

class HasStore b a | b -> a where
  storeLens :: SnapletLens (Snaplet b) (Store a)

data StoreConfig a = StoreConfig
  { storeRoot  :: !FilePath
  , readObject :: !(FilePath -> IO (Maybe a))
    -- | The interval to poll for updates. Measured in seconds.
  , pollEvery  :: !DiffTime
  , logUpdates :: !(Maybe IO.Handle)
  }

-- | A store for all .html files in the root directory and its ancestors
htmlStore :: FilePath -> StoreConfig B.ByteString
htmlStore root = StoreConfig
  { storeRoot  = root
  , readObject = \fs ->
      if ".html" `isSuffixOf` fs || ".htm" `isSuffixOf` fs
      then Just <$> B.readFile fs
      else return Nothing
  , pollEvery  = 60 -- 1 minute
  , logUpdates = Just IO.stdout
  }

-- | Use a lens on an object in the store. Calls 'pass' if the object isn't
-- there.
--
-- Example usage:
--
-- @ storeUse "things/stuff.html" (times.latest) @
storeUse
  :: HasStore b a
  => B.ByteString       -- ^ the key to the object
  -> Getter (Dated a) r -- ^ the lens
  -> Handler b v r
storeUse key l = withTop' storeLens $ do
  obj <- liftIO . lookupStore key =<< get
  case obj of
    Just o  -> return (view l o)
    Nothing -> pass

storeGet :: HasStore b a => B.ByteString -> Handler b v (Dated a)
storeGet key = storeUse key id

storeObject :: HasStore b a => Handler b v (Dated a)
storeObject = storeGet =<< getsRequest rqPathInfo

storeInit
  :: HasStore b a
  => StoreConfig a
  -> SnapletInit b (Store a)
storeInit config = makeSnaplet "store" "Snap store init" Nothing $ do
  let
    log_ :: String -> IO ()
    log_ t = F.for_ (logUpdates config) (\h -> IO.hPutStrLn h t)

  store    <- liftIO (newStore config)
  reloader <- liftIO . forkIO . forever $ do
    threadDelay $! floor (1000000 * toRational (pollEvery config))
    log_ "snaplet-storage: Updating store ..."
    (l, k) <- updateStore config store
    log_ $ "snaplet-storage: Loaded objects: " ++ show l
    log_ $ "snaplet-storage: Kept objects: " ++ show k
    log_ "snaplet-storage: Done"

  onUnload (killThread reloader)

  return store

