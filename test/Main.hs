{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Codec.Compression.LZ4.Conduit (compress)
import           Control.Monad.Trans.Resource (ResourceT, runResourceT)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSL8
import           Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import           Data.List (intersperse)

main :: IO ()
main = do
  x <- runConduit $ yield "hellohellohello" .| compress .| CL.consume
  print x

  let compressToFile :: FilePath -> Source (ResourceT IO) ByteString -> IO ()
      compressToFile path source =
        runResourceT $ runConduit $ source .| compress .| CB.sinkFileCautious path

  compressToFile "out.lz4" $ yield "hellohellohello"

  compressToFile "outbig1.lz4" $
    CL.sourceList $ BSL.toChunks $ BSL8.pack $
      concat $ intersperse " " $ ["BEGIN"] ++ map show [1..100000 :: Int] ++ ["END"]

  compressToFile "outbig2.lz4" $
    CL.sourceList $ BSL.toChunks $ BSL8.pack $
      concat $ intersperse " " $ replicate 100000 "hello"
