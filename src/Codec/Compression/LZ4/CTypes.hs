{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

module Codec.Compression.LZ4.CTypes
  ( LZ4F_cctx
  , LZ4F_preferences_t
  , lz4FrameTypesTable
  ) where

import           Data.Map (Map)
import qualified Data.Map as Map
import qualified Language.C.Types as C
import qualified Language.Haskell.TH as TH

data LZ4F_cctx
data LZ4F_preferences_t

lz4FrameTypesTable :: Map C.TypeSpecifier TH.TypeQ
lz4FrameTypesTable = Map.fromList
  [ (C.TypeName "LZ4F_cctx", [t| LZ4F_cctx |])
  , (C.TypeName "LZ4F_preferences_t", [t| LZ4F_preferences_t |])
  ]
