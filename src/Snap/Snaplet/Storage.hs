{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
module Snap.Snaplet.Storage where
import           Control.Lens
import qualified Data.ByteString       as B
import qualified Data.HashMap.Strict   as H
import           Data.IORef
import           Data.Time
import           System.Directory.Tree

type Map k a = IORef (H.HashMap k a)

data Store a = Store
  { _tracked :: !(Map FilePath UTCTime)
  , _objects :: !(Map B.ByteString a)
  }

makeLenses ''Store

