{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

module Codec.Compression.LZ4.Conduit
  ( Lz4FrameException(..)
  , BlockSizeID(..)
  , BlockMode(..)
  , ContentChecksum(..)
  , FrameType(..)
  , FrameInfo(..)
  , Preferences(..)

  , lz4DefaultPreferences

  , compress
  , compressYieldImmediately
  , compressWithOutBufferSize

  , bsChunksOf
  ) where

import           Control.Exception.Safe (MonadThrow, throwString)
import           Control.Monad (foldM)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Data.ByteString (ByteString, packCStringLen)
import           Data.ByteString.Unsafe (unsafePackCString, unsafeUseAsCStringLen)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import           Data.Conduit
import           Data.Monoid ((<>))
import           Foreign.C.Types (CChar, CSize)
import           Foreign.ForeignPtr (ForeignPtr, addForeignPtrFinalizer, mallocForeignPtr, mallocForeignPtrBytes, finalizeForeignPtr, withForeignPtr)
import           Foreign.Marshal.Alloc (allocaBytes)
import           Foreign.Ptr (Ptr, nullPtr, FunPtr, plusPtr)
import           Foreign.Storable (Storable(..), poke)
import           GHC.Stack (HasCallStack)
import qualified Language.C.Inline as C
import qualified Language.C.Inline.Context as C
import qualified Language.C.Inline.Unsafe as CUnsafe
import           Text.RawString.QQ

import           Codec.Compression.LZ4.CTypes (LZ4F_cctx, lz4FrameTypesTable, Lz4FrameException(..), BlockSizeID(..), BlockMode(..), ContentChecksum(..), FrameType(..), FrameInfo(..), Preferences(..))

#include "lz4frame.h"

C.context (C.baseCtx <> C.fptrCtx <> mempty { C.ctxTypesTable = lz4FrameTypesTable })

C.include "<lz4frame.h>"
C.include "<stdlib.h>"
C.include "<stdio.h>"


newtype Lz4FrameContext = Lz4FrameContext { unLz4FrameContext :: ForeignPtr (Ptr LZ4F_cctx) }
  deriving (Eq, Ord, Show)

newtype Lz4FramePreferencesPtr = Lz4FramePreferencesPtr { unLz4FramePreferencesPtr :: ForeignPtr Preferences }
  deriving (Eq, Ord, Show)


handleLz4Error :: (HasCallStack, MonadIO m, MonadThrow m) => IO CSize -> m CSize
handleLz4Error f = do
  ret <- liftIO f
  -- We use an unsafe foreign call here so that it's fast (it cannot block).
  staticErrMsgPtr <- liftIO [CUnsafe.exp| const char * {
    LZ4F_isError($(size_t ret))
      ? LZ4F_getErrorName($(size_t ret))
      : NULL
  } |]
  if staticErrMsgPtr == nullPtr
    then return ret
    else do
      -- LZ4F error strings are static memory so they don't need to be GC'd,
      -- so we should use `unsafePackCString` here.
      errMsgBs <- liftIO $ unsafePackCString staticErrMsgPtr
      throwString ("lz4frame error: " ++ BS8.unpack errMsgBs)


C.verbatim [r|
void haskell_lz4_freeContext(LZ4F_cctx** ctxPtr)
{
  // We know ctxPtr can be resolved because it was created with
  // mallocForeignPtr, so it is always pointing to something valid
  // during the lifetime of the ForeignPtr (and this function is
  // a finalizer function, which is called only at the very end
  // inside this lifetime).
  LZ4F_cctx* ctx = *ctxPtr;
  // See note [Initialize LZ4 context pointer to NULL]:
  // If ctx is null, we never made a successful call to
  // LZ4F_createCompressionContext().
  // Note at the time of writing the implementation of
  // LZ4F_createCompressionContext() handles null pointers gracefully,
  // but that is an undocumented implementation detail so we don't
  // rely on it here.
  if (ctx != NULL)
  {
    size_t err = LZ4F_freeCompressionContext(ctx);
    if (LZ4F_isError(err))
    {
      fprintf(stderr, "LZ4F_freeCompressionContext failed: %s\n", LZ4F_getErrorName(err));
      exit(1);
    }
  }
}
|]

foreign import ccall "&haskell_lz4_freeContext" haskell_lz4_freeContext :: FunPtr (Ptr (Ptr LZ4F_cctx) -> IO ())

-- TODO Performance:
--      Write a version of `compress` that emits ByteStrings of known
--      constant length. That will allow us to do compression in a zero-copy
--      fashion, writing compressed bytes directly into a the ByteStrings
--      (e.g using `unsafePackMallocCString` or equivalent).
--      We currently don't do that (instead, use allocaBytes + copying packCStringLen)
--      to ensure that the ByteStrings generated are as compact as possible
--      (for the case that `written < size`), as in the current `compress`
--      directly yields the outputs of LZ4F_compressUpdate()
--      (unless they re of 0 length when they are buffered in the context
--      tmp buffer).

-- TODO Try enabling checksums, then corrupt a bit and see if lz4c detects it.


lz4fCreateContext :: (HasCallStack) => IO Lz4FrameContext
lz4fCreateContext = do
  ctxForeignPtr :: ForeignPtr (Ptr LZ4F_cctx) <- mallocForeignPtr
  -- Note [Initialize LZ4 context pointer to NULL]:
  -- We explicitly set it to NULL so that in the finalizer we know
  -- whether there is a context to free or not.
  withForeignPtr ctxForeignPtr $ \ptr -> poke ptr nullPtr

  -- Attach finalizer *before* we call LZ4F_createCompressionContext(),
  -- to ensure there cannot be a time where the context was created
  -- but not finalizer is attached (receiving an async exception at
  -- that time would make us leak memory).
  addForeignPtrFinalizer haskell_lz4_freeContext ctxForeignPtr

  _ <- handleLz4Error [C.block| size_t {
    LZ4F_cctx** ctxPtr = $fptr-ptr:(LZ4F_cctx** ctxForeignPtr);
    LZ4F_errorCode_t err = LZ4F_createCompressionContext(ctxPtr, LZ4F_VERSION);
    return err;
  } |]
  return (Lz4FrameContext ctxForeignPtr)


lz4DefaultPreferences :: Preferences
lz4DefaultPreferences =
  Preferences
    { frameInfo = FrameInfo
      { blockSizeID = LZ4F_default
      , blockMode = LZ4F_blockLinked
      , contentChecksumFlag = LZ4F_noContentChecksum
      , frameType = LZ4F_frame
      , contentSize = 0
      }
    , compressionLevel = 0
    , autoFlush = False
    }


newForeignPtr :: (Storable a) => a -> IO (ForeignPtr a)
newForeignPtr x = do
  fptr <- mallocForeignPtr
  withForeignPtr fptr $ \ptr -> poke ptr x
  return fptr


lz4fCreatePreferences :: IO Lz4FramePreferencesPtr
lz4fCreatePreferences =
  Lz4FramePreferencesPtr <$> newForeignPtr lz4DefaultPreferences


lz4fCompressBegin :: (HasCallStack) => Lz4FrameContext -> Lz4FramePreferencesPtr -> Ptr CChar -> CSize -> IO CSize
lz4fCompressBegin (Lz4FrameContext ctxForeignPtr) (Lz4FramePreferencesPtr prefsForeignPtr) headerBuf headerBufLen = do
  headerSize <- handleLz4Error [C.block| size_t {

    LZ4F_cctx* ctx = *$fptr-ptr:(LZ4F_cctx** ctxForeignPtr);
    LZ4F_preferences_t* lz4_preferences_ptr = $fptr-ptr:(LZ4F_preferences_t* prefsForeignPtr);

    size_t err_or_headerSize = LZ4F_compressBegin(ctx, $(char* headerBuf), $(size_t headerBufLen), lz4_preferences_ptr);
    return err_or_headerSize;
  } |]

  return headerSize


lz4fCompressBound :: (HasCallStack) => CSize -> Lz4FramePreferencesPtr -> IO CSize
lz4fCompressBound srcSize (Lz4FramePreferencesPtr prefsForeignPtr) = do
  handleLz4Error [C.block| size_t {
    size_t err_or_frame_size = LZ4F_compressBound($(size_t srcSize), $fptr-ptr:(LZ4F_preferences_t* prefsForeignPtr));
    return err_or_frame_size;
  } |]


lz4fCompressUpdate :: (HasCallStack) => Lz4FrameContext -> Ptr CChar -> CSize -> Ptr CChar -> CSize -> IO CSize
lz4fCompressUpdate (Lz4FrameContext ctxForeignPtr) destBuf destBufLen srcBuf srcBufLen = do
  -- TODO allow passing in cOptPtr instead of NULL.
  written <- handleLz4Error [C.block| size_t {

    LZ4F_cctx* ctx = *$fptr-ptr:(LZ4F_cctx** ctxForeignPtr);

    size_t err_or_written = LZ4F_compressUpdate(ctx, $(char* destBuf), $(size_t destBufLen), $(char* srcBuf), $(size_t srcBufLen), NULL);
    return err_or_written;
  } |]
  return written


lz4fCompressEnd :: (HasCallStack) => Lz4FrameContext -> Ptr CChar -> CSize -> IO CSize
lz4fCompressEnd (Lz4FrameContext ctxForeignPtr) footerBuf footerBufLen = do
  -- TODO allow passing in cOptPtr instead of NULL.
  footerWritten <- handleLz4Error [C.block| size_t {
    LZ4F_cctx* ctx = *$fptr-ptr:(LZ4F_cctx** ctxForeignPtr);

    size_t err_or_footerWritten = LZ4F_compressEnd(ctx, $(char* footerBuf), $(size_t footerBufLen), NULL);
    return err_or_footerWritten;
  } |]
  return footerWritten


-- Note [Single call to LZ4F_compressUpdate() can create multiple blocks]
-- A single call to LZ4F_compressUpdate() can create multiple blocks,
-- and handles buffers > 32-bit sizes; see:
--   https://github.com/lz4/lz4/blob/52cac9a97342641315c76cfb861206d6acd631a8/lib/lz4frame.c#L601
-- So we don't need to loop around LZ4F_compressUpdate() to compress
-- an arbitrarily large amount of input data, as long as the destination
-- buffer is large enough.


compress :: (MonadIO m, MonadThrow m) => Conduit ByteString m ByteString
compress = compressWithOutBufferSize 0


-- | Compresses the incoming stream of ByteStrings with the lz4 frame format.
--
-- Yields every LZ4 output as a ByteString as soon as the lz4 frame
-- library produces it.
--
-- Note that this does not imply ZL4 frame autoFlush (which affects
-- when the lz4 frame library produces outputs).
compressYieldImmediately :: (MonadIO m, MonadThrow m) => Conduit ByteString m ByteString
compressYieldImmediately = do
  ctx <- liftIO lz4fCreateContext
  prefs <- liftIO lz4fCreatePreferences

  let _LZ4F_HEADER_SIZE_MAX = #{const LZ4F_HEADER_SIZE_MAX}

  -- Header

  headerBs <- liftIO $ allocaBytes (fromIntegral _LZ4F_HEADER_SIZE_MAX) $ \headerBuf -> do
    headerSize <- lz4fCompressBegin ctx prefs headerBuf _LZ4F_HEADER_SIZE_MAX
    packCStringLen (headerBuf, fromIntegral headerSize)

  yield headerBs

  -- Chunks

  awaitForever $ \bs -> do


    m'outBs <- liftIO $ unsafeUseAsCStringLen bs $ \(bsPtr, bsLen) -> do
      let bsLenSize = fromIntegral bsLen

      size <- lz4fCompressBound bsLenSize prefs

      -- TODO Performance: Check if reusing this buffer obtained with `allocaBytes`
      --      makes it faster.
      --      LZ4F_compressBound() always returns a number > the block size (e.g. 256K),
      --      even when the input size passed to it is just a few bytes.
      --      As a result, we allocate at least a full block size each time
      --      (and `allocaBytes` calls `malloc()`), but not using most of it.
      --      Worse, with autoflush=0, most small inputs go into the context buffer,
      --      in which case the `allocaBytes` is completely wasted.
      --      This could be avoided by keeping the last `allocaBytes` buffer around,
      --      and reusing it if it is big enough for the number returned by
      --      LZ4F_compressUpdate() next time.
      --      That should avoid most allocations in the case that we `await` lots of
      --      small ByteStrings.
      m'outBs <- liftIO $ allocaBytes (fromIntegral size) $ \buf -> do

        -- See note [Single call to LZ4F_compressUpdate() can create multiple blocks].
        written <- lz4fCompressUpdate ctx buf size bsPtr bsLenSize

        if written == 0 -- everything fit into the context buffer, no new compressed data was emitted
          then return Nothing
          else Just <$> packCStringLen (buf, fromIntegral written)

      return m'outBs

    case m'outBs of
      Nothing -> return ()
      Just outBs -> yield outBs

  -- Footer

  -- Passing srcSize==0 provides bound for LZ4F_compressEnd(),
  -- see docs of LZ4F_compressBound() for that.
  footerSize <- liftIO $ lz4fCompressBound 0 prefs

  footerBs <- liftIO $ allocaBytes (fromIntegral footerSize) $ \footerBuf -> do
    footerWritten <- lz4fCompressEnd ctx footerBuf footerSize
    packCStringLen (footerBuf, fromIntegral footerWritten)

  yield footerBs

  -- Force resource release here to guarantee memory constantness
  -- of the conduit (and not rely on GC to do it "at some point in the future").
  liftIO $ finalizeForeignPtr (unLz4FramePreferencesPtr prefs)
  liftIO $ finalizeForeignPtr (unLz4FrameContext ctx)


bsChunksOf :: Int -> ByteString -> [ByteString]
bsChunksOf chunkSize bs
  | chunkSize < 1 = error $ "bsChunksOf: chunkSize < 1: " ++ show chunkSize
  | BS.length bs <= chunkSize = [bs]
  | otherwise =
      let (x, rest) = BS.splitAt chunkSize bs in x : bsChunksOf chunkSize rest


-- | Compresses the incoming stream of ByteStrings with the lz4 frame format.
--
-- This function implements two optimisations to reduce unnecessary
-- allocations:
--
-- * Incoming ByteStrings are processed in blocks of 16 KB, allowing us
--   to use a single intermediate output buffer through the lifetime of
--   the conduit.
-- * The `bufferSize` of the output buffer can be increased by the caller
--   up to the given `bufferSize`, to reduce the number of small
--   `ByteString`s being `yield`ed (especially in the case that the
--   input data compresses very well, e.g. a stream of zeros).
--
-- Note that the given `bufferSize` is not a hard limit, it can only be
-- used to *increase* the amount of output buffer we're allowed to use:
-- The function will choose `max(bufferSize, minBufferSizeNeededByLz4)`
-- as the eventual output buffer size.
--
-- Setting `bufferSize = 0` is the legitimate way to set the output buffer
-- size be the minimum required to compress 16 KB inputs and is still a
-- fast default.
compressWithOutBufferSize :: forall m . (MonadIO m, MonadThrow m) => CSize -> Conduit ByteString m ByteString
compressWithOutBufferSize bufferSize = do
  ctx <- liftIO lz4fCreateContext
  prefs <- liftIO lz4fCreatePreferences

  -- We split any incoming ByteString into chunks of this size, so that
  -- we can pass this size to `lz4fCompressBound` once and reuse a buffer
  -- of constant size for the compression.
  let bsInChunkSize = 16*1024

  compressBound <- liftIO $ lz4fCompressBound (#{const LZ4F_HEADER_SIZE_MAX} + bsInChunkSize) prefs
  let outBufferSize = max bufferSize compressBound

  outBuf <- liftIO $ mallocForeignPtrBytes (fromIntegral outBufferSize)
  let withOutBuf f = liftIO $ withForeignPtr outBuf f

  headerSize <- withOutBuf $ \buf -> lz4fCompressBegin ctx prefs buf outBufferSize

  let loop remainingCapacity = do
        await >>= \case
          Nothing -> do
            let offset = fromIntegral $ outBufferSize - remainingCapacity
            footerWritten <- withOutBuf $ \buf -> lz4fCompressEnd ctx (buf `plusPtr` offset) remainingCapacity
            let outBufLen = outBufferSize - remainingCapacity + footerWritten

            outBs <- withOutBuf $ \buf -> packCStringLen (buf, fromIntegral outBufLen)
            yield outBs

          Just bs -> do
            let bss = bsChunksOf (fromIntegral bsInChunkSize) bs
            newRemainingCapacity <- foldM (\cap subBs -> loopSingleBs cap subBs) remainingCapacity bss
            loop newRemainingCapacity

      loopSingleBs :: CSize -> ByteString -> ConduitM i ByteString m CSize
      loopSingleBs remainingCapacity bs
        | remainingCapacity < compressBound = do
            -- Not enough space in outBuf to guarantee that the next call
            -- to `lz4fCompressUpdate` will fit; so yield (a copy of) the
            -- current outBuf, then it's all free again.
            outBs <- withOutBuf $ \buf -> packCStringLen (buf, fromIntegral (outBufferSize - remainingCapacity))
            yield outBs
            loopSingleBs outBufferSize bs
        | otherwise = do
            written <- liftIO $ unsafeUseAsCStringLen bs $ \(bsPtr, bsLen) -> do
              let bsLenSize = fromIntegral bsLen

              -- See note [Single call to LZ4F_compressUpdate() can create multiple blocks]
              let offset = fromIntegral $ outBufferSize - remainingCapacity
              withOutBuf $ \buf -> lz4fCompressUpdate ctx (buf `plusPtr` offset) remainingCapacity bsPtr bsLenSize

            let newRemainingCapacity = remainingCapacity - written
            -- TODO assert newRemainingCapacity > 0
            let writtenInt = fromIntegral written

            if
              | written == 0              -> return newRemainingCapacity
              | writtenInt < BS.length bs -> loopSingleBs newRemainingCapacity (BS.drop writtenInt bs)
              | otherwise                 -> return newRemainingCapacity

  loop (outBufferSize - headerSize)

  -- Force resource release here to guarantee memory constantness
  -- of the conduit (and not rely on GC to do it "at some point in the future").
  liftIO $ finalizeForeignPtr outBuf
  liftIO $ finalizeForeignPtr (unLz4FramePreferencesPtr prefs)
  liftIO $ finalizeForeignPtr (unLz4FrameContext ctx)
