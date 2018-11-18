{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Codec.Compression.LZ4.Conduit (compress, decompress, bsChunksOf)
import           Control.Monad (when)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSL8
import           Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Process as CP
import           Data.List (intersperse)
import           Test.Hspec


runCompressToLZ4 :: ConduitT () ByteString IO () -> IO ByteString
runCompressToLZ4 source = do
  (_, result, _) <- CP.sourceCmdWithStreams "lz4 -d" (source .| compress) CL.consume CL.consume
  return $ BS.concat result

runLZ4ToDecompress :: ConduitT () ByteString IO () -> IO ByteString
runLZ4ToDecompress source = do
  (_, result, _) <- CP.sourceCmdWithStreams "lz4 -c" source (decompress .| CL.consume) CL.consume
  return $ BS.concat result

main :: IO ()
main = do
  when (bsChunksOf 3 "abc123def4567" /= ["abc", "123", "def", "456", "7"]) $
    error "bsChunksOf failed"

  let prepare :: [BSL.ByteString] -> [ByteString]
      prepare strings = BSL.toChunks $ BSL.concat $ intersperse " " $ ["BEGIN"] ++ strings ++ ["END"]

  hspec $ do
    describe "Compression" $ do
      it "compresses simple string" $ do
        let string = "hellohellohellohello"
        actual <- runCompressToLZ4 (yield string)
        actual `shouldBe` string

      it "compresses 100000 integers" $ do
        let strings = prepare $ map (BSL8.pack . show) [1..100000 :: Int]
        actual <- runCompressToLZ4 (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

      it "compresses 100000 strings" $ do
        let strings = prepare $ replicate 100000 "hello" 
        actual <- runCompressToLZ4 (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

    describe "Decompression" $ do
      it "decompresses simple string" $ do
        let string = "hellohellohellohello"
        actual <- runLZ4ToDecompress (yield string)
        actual `shouldBe` string

      it "decompresses 100000 integers" $ do
        let strings = prepare $ map (BSL8.pack . show) [1..100000 :: Int]
        actual <- runLZ4ToDecompress (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

      it "decompresses 100000 strings" $ do
        let strings = prepare $ replicate 100000 "hello" 
        actual <- runLZ4ToDecompress (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)
