{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Control.Monad (replicateM)
import           Control.Monad.IO.Unlift (MonadUnliftIO)
import           Control.Monad.ST (runST)
import           Control.Monad.Trans.Resource (ResourceT, runResourceT)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import           Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Process as CP
import           Data.List (intersperse, foldl')
import           Data.Monoid ((<>))
import qualified Data.Vector.Storable as VS
import           Data.Vector.Storable.ByteString (vectorToByteString)
import           Data.Word (Word64)
import           System.IO (hSetBuffering, stdout, BufferMode(LineBuffering))
import qualified System.Random.PCG as PCG
import           Test.Hspec
import           Test.Hspec.QuickCheck (modifyMaxSize)
import qualified Test.QuickCheck as QC
import qualified Test.QuickCheck.Monadic as QCM

import           Codec.Compression.LZ4.Conduit (compress, decompress, bsChunksOf, BlockSizeID(LZ4F_default), blockSizeBytes)


runCompressToLZ4 :: (MonadUnliftIO m) => ConduitT () ByteString (ResourceT m) () -> m ByteString
runCompressToLZ4 source = runResourceT $ do

  (_, result, _) <- CP.sourceCmdWithStreams "lz4 -d" (source .| compress) CL.consume CL.consume
  return $ BS.concat result

runLZ4ToDecompress :: (MonadUnliftIO m) => ConduitT () ByteString (ResourceT m) () -> m ByteString
runLZ4ToDecompress source = runResourceT $ do
  (_, result, _) <- CP.sourceCmdWithStreams "lz4 -c" source (decompress .| CL.consume) CL.consume
  return $ BS.concat result


-- Note: QuickCheck's generation of random (Byte)Strings is
-- very slow, around ~1 MB/s on my laptop (i5-2520M)
-- (/dev/urandom does 230 MB/s there).
-- I suspect this is due to `random` or `tf-random` being that slow.
-- So that means we can't use it to generate lots of large
-- ByteStrings for testing lz4.
-- Instead, we use pcg-random here (which does ~300 MB/s on my laptop)
-- fed with a length and a seed given by QuickCheck.

-- | Makes a random `ByteString` using pcg-random (high throughput)
-- from a given length and seed.
mkRandomBytesOfLength :: Int -> Word64 -> ByteString
mkRandomBytesOfLength len seed = runST $ do
  gen <- PCG.initialize seed 0 -- we only use a 64 bit seed
  vec <- VS.replicateM ((len + 7) `div` 8) (PCG.uniform gen)
  return $ BS.take len $ vectorToByteString (vec :: VS.Vector Word64)

-- | Generates a list of `ByteString`s for testing.
-- The total amount of bytes generated is <= `QC.getSize` many MBs.
bytestringListGen :: QC.Gen [ByteString]
bytestringListGen = do
  size <- QC.getSize
  let maxTotalBytes = size * 1024 * 1024 -- for size=100, this is 100 MB
  numYields <- QC.choose (0, size)
  replicateM numYields $ do
    maxSize <- QC.choose (0, maxTotalBytes `div` numYields)
    len <- QC.choose (0, maxSize)
    seed <- QC.choose (minBound, maxBound)
    return $ mkRandomBytesOfLength len seed


-- | `QC.shrink` implementation tuned for finding
-- smallest-still-failing inputs to lz4 quickly.
--
-- The default `shrink` implementation for `[ByteString]`
-- is way too slow and makes QuickCheck hang forever when
-- a failure is found on large inputs.
bytestringListShrinker :: [ByteString] -> [[ByteString]]
bytestringListShrinker strs =
  -- Drop any string
  [dropNth i strs | i <- [0..(length strs - 1)]]
  ++
  -- Merge any two subsequent strings
  [mergeNth i strs | i <- [0..(length strs - 2)]]
  ++
  -- Shorten any individual string.
  -- First try shortening a lot, then less down to 1-byte cutoffs,
  -- in powers of 10.
  [ [ BS.drop powerOf10 s | s <- strs ]
  | let n = foldl' max 0 (map BS.length strs)
  , powerOf10 <- descendingPowersOf10SmallerThan n
  ]
  ++
  -- Replace all chars in all strings by 'a' to make them simpler.
  -- This could be done with more sophistication, e.g. by replacing
  -- more and more strings by 'a' (by chunks growing exponentially),
  -- instead of all of them in one step.
  [[ replaceAllCharsBy_a s | s <- strs, not (BS.all (== 97) s)]] -- 97 == 'a'
  where
    dropNth :: Int -> [a] -> [a]
    dropNth i list =
      let (left, _:right) = splitAt i list
      in left ++ right

    mergeNth :: (Monoid m) => Int -> [m] -> [m]
    mergeNth i list =
      let (left, x:y:right) = splitAt i list
      in left ++ [x <> y] ++ right

    descendingPowersOf10SmallerThan :: Int -> [Int]
    descendingPowersOf10SmallerThan n =
      reverse (takeWhile (< n) $ iterate (*10) 1)

    replaceAllCharsBy_a :: ByteString -> ByteString
    replaceAllCharsBy_a bs = BS.map (const 97) bs -- 97 == 'a'


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering

  hspec $ do
    describe "bsChunksOf" $ do
      it "chunks up a string" $ do
        bsChunksOf 3 "abc123def4567" `shouldBe` ["abc", "123", "def", "456", "7"]

    describe "BlockSizeID" $ do
      it "blockSizeBytes works on LZ4F_default" $ do
        -- Shouldn't loop forever
        blockSizeBytes LZ4F_default `seq` return () :: Expectation

    describe "Compression" $ do
      it "compresses simple string" $ do
        let string = "hellohellohellohello"
        actual <- runCompressToLZ4 (yield string)
        actual `shouldBe` string

      it "compresses 100000 yielded integers" $ do
        let strings = intersperse " " $ map (BS8.pack . show) [1..100000 :: Int]
        actual <- runCompressToLZ4 (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

      it "compresses 100000 yielded short strings" $ do
        let strings = intersperse " " $ replicate 100000 "hello"
        actual <- runCompressToLZ4 (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

      it "compresses the concatenation of 100000 short strings" $ do
        let strings = [BS.concat $ replicate 100000 "hello"]
        actual <- runCompressToLZ4 (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

      it "compresses the concatenation of 1000000 short strings" $ do
        let strings = [BS.concat $ replicate 1000000 "hello"]
        actual <- runCompressToLZ4 (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

    describe "Decompression" $ do
      it "decompresses simple string" $ do
        let string = "hellohellohellohello"
        actual <- runLZ4ToDecompress (yield string)
        actual `shouldBe` string

      it "decompresses 100000 yielded integers" $ do
        let strings = map (BS8.pack . show) [1..100000 :: Int]
        actual <- runLZ4ToDecompress (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

      it "decompresses 100000 yielded short strings" $ do
        let strings = replicate 100000 "hello"
        actual <- runLZ4ToDecompress (CL.sourceList strings)
        actual `shouldBe` (BS.concat strings)

    describe "Identity" $ do

      modifyMaxSize (const 100) $ it "compress and decompress arbitrary strings"$
        QC.property $ do

          QC.forAllShrink bytestringListGen bytestringListShrinker $ \bsList -> do

            QCM.monadicIO $ do
              -- Reporting
              size <- QCM.pick QC.getSize
              let details str = QCM.monitor (QC.counterexample str)
              details $ "QuickCheck size: " ++ show size
              details $ "Input lengths: " ++ show (map BS.length bsList)

              -- Debugging QuickCheck hangs:
              -- Uncomment these to debug QuickCheck hanging during `shrink`
              --QCM.run $ hPutStrLn stderr (show $ map BS.length bsList)
              --QCM.run $ hPutStrLn stderr (show $ sum $ map BS.length bsList)

              actual <- QCM.run (runConduitRes $ CL.sourceList bsList .| compress .| decompress .| CL.consume)
              QCM.assert (BS.concat bsList == BS.concat actual)
