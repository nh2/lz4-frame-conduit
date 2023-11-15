{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module Main (main) where

import           Codec.Compression.LZ4.Conduit (compress, compressMultiFrame, decompress, bsChunksOf)
import           Conduit
import           Control.Monad (when, void)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSL8
import           Data.Conduit.Binary as CB
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Process as CP
import           Data.List (intersperse)
import           Test.Hspec
import           Test.Hspec.QuickCheck (modifyMaxSize, prop)
import qualified Test.QuickCheck as QC
import qualified Test.QuickCheck.Monadic as QCM



runCompressToLZ4 :: (MonadUnliftIO m) => ConduitT () ByteString (ResourceT m) () -> m ByteString
runCompressToLZ4 source = runResourceT $ do

  (_, result, _) <- CP.sourceCmdWithStreams "lz4 -d" (source .| compress) foldC foldC
  return result

-- | Create a new LZ4 frame every len bytes.
createFramesOfCE :: (MonadIO m, Monad m) => Int -> ConduitT ByteString (Flush ByteString) m ()
createFramesOfCE len = chunksOfCE len .| awaitForever (\x -> yield (Chunk x) >> yield Flush)

runCompressMultiFrameToLZ4 :: (MonadUnliftIO m) => ConduitT () ByteString (ResourceT m) () -> m ByteString
runCompressMultiFrameToLZ4 source = runResourceT $ do
  (_, result, _) <- CP.sourceCmdWithStreams "lz4 -d" (source .| createFramesOfCE 1000 .| void compressMultiFrame) foldC foldC
  return result

runLZ4ToDecompress :: (MonadUnliftIO m) => ConduitT () ByteString (ResourceT m) () -> m ByteString
runLZ4ToDecompress source = runResourceT $ do
  (_, result, _) <- CP.sourceCmdWithStreams "lz4 -c" source (decompress .| foldC) foldC
  return result

main :: IO ()
main = do
  -- Big memory tests are disabled by default to be kind to packagers and CI.
  let skipBigmemTests = True
      skipBigmemTest = when skipBigmemTests $ pendingWith "skipped by default due to big RAM requirement"

  let prepare :: [BSL.ByteString] -> [ByteString]
      prepare strings = BSL.toChunks $ BSL.concat $ intersperse " " $ ["BEGIN"] ++ strings ++ ["END"]

  hspec $ do
    describe "bsChunksOf" $ do
      it "chunks up a string" $ do
        bsChunksOf 3 "abc123def4567" `shouldBe` ["abc", "123", "def", "456", "7"]

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

      it "compresses 1MB ByteString" $ do
        let bs = BS.replicate 100000 42
        actual <- runCompressToLZ4 (CB.sourceLbs $ BSL.fromStrict bs)
        actual `shouldBe` bs

      it "compresses 5GiB ByteString" $ do -- more than 32-bit many Bytes
        skipBigmemTest
        let bs = BS.replicate (5 * 1024*1024*1024) 42
        actual <- runCompressToLZ4 (CB.sourceLbs $ BSL.fromStrict bs)
        actual `shouldBe` bs

    describe "Compression MultiFrame" $ do
      it "compresses simple string" $ do
        let string = "hellohellohellohello"
        actual <- runCompressMultiFrameToLZ4 (yield string)
        actual `shouldBe` string

      it "compresses 100000 integers" $ do
        let strings = prepare $ map (BSL8.pack . show) [1..100000 :: Int]
        actual <- runCompressMultiFrameToLZ4 (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

      it "compresses 100000 strings" $ do
        let strings = prepare $ replicate 100000 "hello"
        actual <- runCompressMultiFrameToLZ4 (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

      it "compresses 1MB ByteString" $ do
        let bs = BS.replicate 100000 42
        actual <- runCompressToLZ4 (CB.sourceLbs $ BSL.fromStrict bs)
        actual `shouldBe` bs

      it "compresses 5GiB ByteString" $ do -- more than 32-bit many Bytes
        skipBigmemTest
        let bs = BS.replicate (5 * 1024*1024*1024) 42
        actual <- runCompressMultiFrameToLZ4 (CB.sourceLbs $ BSL.fromStrict bs)
        actual `shouldBe` bs

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

      it "decompresses 1MB ByteString" $ do
        let bs = BS.replicate 100000 42
        actual <- runLZ4ToDecompress (CB.sourceLbs $ BSL.fromStrict bs)
        actual `shouldBe` bs

      it "decompresses 5GiB ByteString" $ do -- more than 32-bit many Bytes
        skipBigmemTest
        let bs = BS.replicate (5 * 1024*1024*1024) 42
        actual <- runLZ4ToDecompress (CB.sourceLbs $ BSL.fromStrict bs)
        actual `shouldBe` bs

    describe "Identity" $ do
      modifyMaxSize (const 10000) $ it "compress and decompress arbitrary strings"$
        QC.property $ \string -> QCM.monadicIO $ do
          let bs = BSL.toChunks $ BSL8.pack string
          actual <- QCM.run (runConduitRes $ CL.sourceList bs .| compress .| decompress .| CL.consume)
          QCM.assert(BS.concat bs == BS.concat actual)

    describe "Identity MultiFrame" $
      modifyMaxSize (const 10000) $ prop "compress and decompress arbitrary strings" $ \string -> do
          let bs = BS.pack string
          actual <- runConduitRes $ yield bs .| createFramesOfCE 10 .| void compressMultiFrame .| decompress .| foldC
          actual `shouldBe` bs

    describe "Slice a multiframe" $
        it "Decompress only the second frame." $  do
            let bs = BS.pack $ replicate 10 0 ++ replicate 10 1 ++ replicate 10 2
            (offsets, cbs) <- runConduitRes $ yield bs .| createFramesOfCE 10 .| compressMultiFrame `fuseBoth` foldC
            let mark = offsets !! 1
                mark2 = offsets !! 2
            actual <- runConduitRes $ yield cbs .| (dropCE mark >> takeCE (mark2 - mark) .| decompress .| foldC)
            actual `shouldBe` (BS.pack $ replicate 10 1)
