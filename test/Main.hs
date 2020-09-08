{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Control.Monad (forever)
import           Codec.Compression.LZ4.Conduit (compress, decompress, bsChunksOf)
import           Control.Monad.IO.Unlift (MonadUnliftIO, liftIO)
import           Control.Monad.Trans.Resource (ResourceT, runResourceT)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSL8
import           Data.Conduit
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Process as CP
import           Data.List (intersperse)
import           Test.Hspec
import           Test.Hspec.QuickCheck (modifyMaxSize)
import qualified Test.QuickCheck as QC
import qualified Test.QuickCheck.Monadic as QCM



runCompressToLZ4 :: (MonadUnliftIO m) => ConduitT () ByteString (ResourceT m) () -> m ByteString
runCompressToLZ4 source = runResourceT $ do

  (_, result, _) <- CP.sourceCmdWithStreams "lz4 -d" (source .| compress) CL.consume CL.consume
  return $ BS.concat result

runLZ4ToDecompress :: (MonadUnliftIO m) => ConduitT () ByteString (ResourceT m) () -> m ByteString
runLZ4ToDecompress source = runResourceT $ do
  (_, result, _) <- CP.sourceCmdWithStreams "lz4 -c" source (decompress .| CL.consume) CL.consume
  return $ BS.concat result

main :: IO ()
main = do
  let prepare :: [BSL.ByteString] -> [ByteString]
      prepare strings = BSL.toChunks $ BSL.concat $ intersperse " " $ ["BEGIN"] ++ strings ++ ["END"]

  hspec $ do

    describe "reproducing memory error" $ do

      it "compresses 1000 strings" $ do
        let strings = prepare $ replicate 1000 "hello" -- cannot reproduce if `!`ing this
        actual <- runCompressToLZ4 (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

    describe "more reproducing" $ do

      it "decompresses 10000 strings" $ do
        let !strings = prepare $ replicate 10000 "hello"
        actual <- runLZ4ToDecompress (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

    -- describe "reproducing memory error" $ do

    --   it "test" $ do

    --     forever $ do
    --       return () :: IO ()

    --       let !strings = prepare $ replicate 1000 "hello"
    --       actual <- runCompressToLZ4 (CL.sourceList strings)
    --       actual `shouldBe` (BS.concat strings)

    --       let !strings = prepare $ replicate 10000 "hello"
    --       actual <- runLZ4ToDecompress (CL.sourceList strings)
    --       actual `shouldBe` (BS.concat strings)

    --     return ()
