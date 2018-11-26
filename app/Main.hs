{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Control.Applicative (optional)
import           Control.Monad (unless)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Trans.Resource (MonadResource, runResourceT)
import qualified Data.ByteString as BS
import           Data.Conduit
import qualified Data.Conduit.Binary as CB
import           Data.Monoid ((<>))
import           Data.Text (Text)
import qualified Data.Text as T
import           Options.Applicative (Parser, ReadM, argument, eitherReader, str, metavar, long, short)
import qualified Options.Applicative as Opts
import           System.IO (stdin, stdout)
import qualified System.IO as IO
import           Text.Read (readMaybe)

import qualified Codec.Compression.LZ4.Conduit as LZ4F


data CompressMode
  = CompressNoBuffering
  | CompressYieldImmediately
  | CompressWithBufferSize Int
  deriving (Eq, Ord, Show)


-- | Command line arguments of this program.
data CLIArgs = CLIArgs
  { m'inputFile :: Maybe Text
  , m'outputFile :: Maybe Text
  , decompress :: Bool
  , compressMode :: CompressMode
  } deriving (Eq, Ord, Show)


compressModeReadM :: ReadM CompressMode
compressModeReadM = eitherReader $ \case
  "noBuffering" -> pure CompressNoBuffering
  "immediately" -> pure CompressYieldImmediately
  s -> case readMaybe s of
    Just num -> pure $ CompressWithBufferSize num
    Nothing -> fail $ "bad --compress-mode: " ++ s


cliArgsParser :: Parser CLIArgs
cliArgsParser = CLIArgs
  <$> optional (T.pack <$> argument str (metavar "INPUT_FILE"))
  <*> optional (T.pack <$> argument str (metavar "OUTPUT-FILE"))
  <*> Opts.switch (long "decompress" <> short 'd')
  <*> Opts.option compressModeReadM
    (    long "compress-mode"
      <> Opts.value (CompressWithBufferSize 4194304)
      <> Opts.help "One of [noBuffering, immediately, N], where N is a number for buffering. Defaults to 4194304 (4 MB)"
    )


-- | Parses the command line arguments for this program.
parseArgs :: IO CLIArgs
parseArgs = Opts.execParser $
  Opts.info
    (Opts.helper <*> cliArgsParser)
    (Opts.fullDesc <> Opts.progDesc "Compress or decompress .lz4 files")


newtype ReadHandle = ReadHandle IO.Handle

openFile :: FilePath -> IO ReadHandle
openFile fp = ReadHandle `fmap` IO.openBinaryFile fp IO.ReadMode

closeFile :: ReadHandle -> IO ()
closeFile (ReadHandle h) = IO.hClose h

readChunkSize :: Int -> ReadHandle -> IO BS.ByteString
readChunkSize chunkSize (ReadHandle h) = do
  got <- BS.hGet h chunkSize
  return got


sourceFileWithChunkSize :: (MonadResource m) => Int -> FilePath -> ConduitT i BS.ByteString m ()
sourceFileWithChunkSize chunkSize fp =
    bracketP
        (openFile fp)
         closeFile
         loop
  where
    loop h = do
        bs <- liftIO $ readChunkSize chunkSize h
        unless (BS.null bs) $ do
            yield bs
            loop h


sourceHandleWithChunkSize :: (MonadIO m) => Int -> IO.Handle -> ConduitT i BS.ByteString m ()
sourceHandleWithChunkSize chunkSize h =
    loop
  where
    loop = do
        bs <- liftIO (BS.hGet h chunkSize)
        if BS.null bs
            then return ()
            else yield bs >> loop


main :: IO ()
main = do
  CLIArgs
    { m'inputFile
    , m'outputFile
    , decompress
    , compressMode
    } <- parseArgs

  let prefs =
        LZ4F.lz4DefaultPreferences
          { LZ4F.frameInfo = (LZ4F.frameInfo LZ4F.lz4DefaultPreferences)
              -- The lz4 CLI utility defaults to 4 MB as time of writing.
              { LZ4F.blockSizeID = LZ4F.LZ4F_max4MB
              }
          }

  let source = case m'inputFile of
        Nothing -> sourceHandleWithChunkSize (4 * 1024 * 1024) stdin
        Just "-" -> sourceHandleWithChunkSize (4 * 1024 * 1024) stdin
        Just path -> sourceFileWithChunkSize (4 * 1024 * 1024) (T.unpack path)

  let sink = case m'outputFile of
        Nothing -> CB.sinkHandle stdout
        Just "-" -> CB.sinkHandle stdout
        Just path -> CB.sinkFile (T.unpack path)

  let compressOrDecompress
        | decompress = LZ4F.decompress
        | otherwise  = case compressMode of
            CompressNoBuffering -> LZ4F.compressNoBuffering prefs
            CompressYieldImmediately -> LZ4F.compressYieldImmediately prefs
            CompressWithBufferSize n -> LZ4F.compressWithOutBufferSize (fromIntegral n) prefs

  runResourceT $ runConduit $ source .| compressOrDecompress .| sink
