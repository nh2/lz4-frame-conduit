{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-partial-type-signatures #-}

-- | Conduit implementing the official LZ4 frame streaming format.
--
-- == Note on @unsafe@ FFI
--
-- All LZ4 C functions are supposed to be CPU-bound, do not perform
-- any form of blocking IO, and there are no callbacks back into Haskell.
-- Correspondingly, we use unsafe FFI calls everywhere.
module Codec.Compression.LZ4.Conduit
  ( Lz4FrameException(..)
  , BlockSizeID(..)
  , BlockMode(..)
  , ContentChecksum(..)
  , FrameType(..)
  , FrameInfo(..)
  , Preferences(..)

  , lz4DefaultPreferences
  , blockSizeBytes

  , compress
  , compressNoBuffering
  , compressYieldImmediately
  , compressWithOutBufferSize

  , decompress

  , bsChunksOf

  -- * Internals
  , Lz4FrameCompressionContext(..)
  , ScopedLz4FrameCompressionContext(..)
  , ScopedLz4FramePreferencesPtr(..)
  , Lz4FrameDecompressionContext(..)
  , lz4fCreateCompressonContext
  , lz4fCreateDecompressionContext
  , withScopedLz4fPreferences
  , withScopedLz4fCompressionContext
  ) where

import           UnliftIO.Exception (throwString, bracket)
import           UnliftIO.Foreign (addForeignPtrFinalizer, alloca, allocaBytes, finalizeForeignPtr, ForeignPtr, free, FunPtr, malloc, mallocArray, mallocForeignPtr, mallocForeignPtrBytes, new, nullPtr, plusPtr, poke, Ptr, reallocArray, Storable(..), with, withForeignPtr)
import           Control.Monad (foldM, when)
import           Control.Monad.IO.Unlift (MonadUnliftIO)
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Trans.Resource (MonadResource)
import           Data.Bits (testBit)
import           Data.ByteString (ByteString, packCStringLen)
import           Data.ByteString.Unsafe (unsafePackCString, unsafeUseAsCStringLen)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as BSL
import           Data.Conduit
import qualified Data.Conduit.Binary as CB
import           Data.List (delete)
import           Data.Monoid ((<>))
import           Data.Traversable (for)
import           Foreign.C.Types (CChar, CSize)
import           GHC.Stack (HasCallStack)
import qualified Language.C.Inline as C
import qualified Language.C.Inline.Context as C
import qualified Language.C.Inline.Unsafe as CUnsafe
import           System.IO.Unsafe (unsafePerformIO)
import           Text.RawString.QQ

import           Codec.Compression.LZ4.CTypes (LZ4F_cctx, LZ4F_dctx, lz4FrameTypesTable, Lz4FrameException(..), BlockSizeID(..), BlockMode(..), ContentChecksum(..), FrameType(..), FrameInfo(..), Preferences(..))

#include "lz4frame.h"

C.context (C.baseCtx <> C.fptrCtx <> mempty { C.ctxTypesTable = lz4FrameTypesTable })

C.include "<lz4frame.h>"
C.include "<stdlib.h>"
C.include "<stdio.h>"


newtype Lz4FrameCompressionContext = Lz4FrameCompressionContext { unLz4FrameCompressionContext :: ForeignPtr (Ptr LZ4F_cctx) }
  deriving (Eq, Ord, Show)

newtype ScopedLz4FrameCompressionContext = ScopedLz4FrameCompressionContext { unScopedLz4FrameCompressionContext :: Ptr LZ4F_cctx }
  deriving (Eq, Ord, Show)

newtype ScopedLz4FramePreferencesPtr = ScopedLz4FramePreferencesPtr { unScopedLz4FramePreferencesPtr :: Ptr Preferences }
  deriving (Eq, Ord, Show)

newtype Lz4FrameDecompressionContext = Lz4FrameDecompressionContext { unLz4FrameDecompressionContext :: ForeignPtr (Ptr LZ4F_dctx) }
  deriving (Eq, Ord, Show)


handleLz4Error :: (HasCallStack, MonadUnliftIO m) => IO CSize -> m CSize
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
void haskell_lz4_freeCompressionContext(LZ4F_cctx** ctxPtr)
{
  // We know ctxPtr can be dereferenced because it was created with
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

foreign import ccall "&haskell_lz4_freeCompressionContext" haskell_lz4_freeCompressionContext :: FunPtr (Ptr (Ptr LZ4F_cctx) -> IO ())


allocateLz4fScopedCompressionContext :: IO ScopedLz4FrameCompressionContext
allocateLz4fScopedCompressionContext = do
  alloca $ \(ctxPtrPtr :: Ptr (Ptr LZ4F_cctx)) -> do
    _ <- handleLz4Error [CUnsafe.block| size_t {
      LZ4F_cctx** ctxPtr = $(LZ4F_cctx** ctxPtrPtr);
      LZ4F_errorCode_t err = LZ4F_createCompressionContext(ctxPtr, LZ4F_VERSION);
      return err;
    } |]
    ctxPtr <- peek ctxPtrPtr
    return (ScopedLz4FrameCompressionContext ctxPtr)


freeLz4ScopedCompressionContext :: ScopedLz4FrameCompressionContext -> IO ()
freeLz4ScopedCompressionContext (ScopedLz4FrameCompressionContext ctxPtr) = do
  _ <- handleLz4Error
    [CUnsafe.block| size_t {
      return LZ4F_freeCompressionContext($(LZ4F_cctx* ctxPtr));
    } |]
  return ()



-- TODO Performance:
--      Write a version of `compress` that emits ByteStrings of known
--      constant length. That will allow us to do compression in a zero-copy
--      fashion, writing compressed bytes directly into a the ByteStrings
--      (e.g using `unsafePackMallocCString` or equivalent).
--      We currently don't do that (instead, use allocaBytes + copying packCStringLen)
--      to ensure that the ByteStrings generated are as compact as possible
--      (for the case that `written < size`), since the current `compress`
--      conduit directly yields the outputs of LZ4F_compressUpdate()
--      (unless they are of 0 length when they are buffered in the context
--      tmp buffer).

-- TODO Try enabling checksums, then corrupt a bit and see if lz4c detects it.

-- TODO Add `with*` style bracketed functions for creating the
--      LZ4F_createCompressionContext and Lz4FramePreferencesPtr
--      for prompt resource release,
--      in addition to the GC'd variants below.
--      This would replace our use of `finalizeForeignPtr` in the conduit.
--      `finalizeForeignPtr` seems almost as good, but note that it
--      doesn't guarantee prompt resource release on exceptions;
--      a `with*` style function that uses `bracket` does.
--      However, it isn't clear yet which one would be faster
--      (what the cost of `mask` is compared to foreign pointer finalizers).
--      Also note that prompt freeing has side benefits,
--      such as reduced malloc() fragmentation (the closer malloc()
--      and free() are to each other, the smaller is the chance to
--      have malloc()s on top of the our malloc() in the heap,
--      thus the smaller the chance that we cannot decrease the
--      heap pointer upon free() (because "mallocs on top" render
--      heap memory unreturnable to the OS; memory fragmentation).


-- TODO Turn the above TODO into documentation


withScopedLz4fCompressionContext :: (MonadUnliftIO m, HasCallStack) => (ScopedLz4FrameCompressionContext -> m a) -> m a
withScopedLz4fCompressionContext f =
  bracket
    (liftIO allocateLz4fScopedCompressionContext)
    (liftIO . freeLz4ScopedCompressionContext)
    f


lz4fCreateCompressonContext :: (HasCallStack) => IO Lz4FrameCompressionContext
lz4fCreateCompressonContext = do
  ctxForeignPtr :: ForeignPtr (Ptr LZ4F_cctx) <- mallocForeignPtr
  -- Note [Initialize LZ4 context pointer to NULL]:
  -- We explicitly set it to NULL so that in the finalizer we know
  -- whether there is a context to free or not.
  withForeignPtr ctxForeignPtr $ \ptr -> poke ptr nullPtr

  -- Attach finalizer *before* we call LZ4F_createCompressionContext(),
  -- to ensure there cannot be a time where the context was created
  -- but not finalizer is attached (receiving an async exception at
  -- that time would make us leak memory).
  addForeignPtrFinalizer haskell_lz4_freeCompressionContext ctxForeignPtr
  -- TODO The whole idea above is to avoid `mask`.
  --      But we should check if `addForeignPtrFinalizer` itself is actually
  --      async exception safe; if not, this is pointless.

  _ <- handleLz4Error [CUnsafe.block| size_t {
    LZ4F_cctx** ctxPtr = $fptr-ptr:(LZ4F_cctx** ctxForeignPtr);
    LZ4F_errorCode_t err = LZ4F_createCompressionContext(ctxPtr, LZ4F_VERSION);
    return err;
  } |]
  return (Lz4FrameCompressionContext ctxForeignPtr)


lz4DefaultPreferences :: Preferences
lz4DefaultPreferences =
  Preferences
    { frameInfo = FrameInfo
      { blockSizeID = LZ4F_default
      -- The above is the default of the lz4 library, but
      -- 4 MB is the default of the lz4 CLI utility (as of writing).
      , blockMode = LZ4F_blockLinked
      , contentChecksumFlag = LZ4F_noContentChecksum
      , frameType = LZ4F_frame
      , contentSize = 0
      }
    , compressionLevel = 0
    , autoFlush = False
    }


withScopedLz4fPreferences :: (MonadUnliftIO m, HasCallStack) => Preferences -> (ScopedLz4FramePreferencesPtr -> m a) -> m a
withScopedLz4fPreferences prefs f =
  bracket
    (ScopedLz4FramePreferencesPtr <$> new prefs)
    (\(ScopedLz4FramePreferencesPtr ptr) -> free ptr)
    f


-- Simple API
lz4fCompressFrameBound :: (HasCallStack) => CSize -> ScopedLz4FramePreferencesPtr -> IO CSize
lz4fCompressFrameBound srcSize (ScopedLz4FramePreferencesPtr prefsPtr) = do
  handleLz4Error [CUnsafe.block| size_t {
    size_t err_or_frame_size = LZ4F_compressFrameBound($(size_t srcSize), $(LZ4F_preferences_t* prefsPtr));
    return err_or_frame_size;
  } |]


lz4fCompressBegin :: (HasCallStack) => ScopedLz4FrameCompressionContext -> ScopedLz4FramePreferencesPtr -> Ptr CChar -> CSize -> IO CSize
lz4fCompressBegin (ScopedLz4FrameCompressionContext ctx) (ScopedLz4FramePreferencesPtr prefsPtr) headerBuf headerBufLen = do
  headerSize <- handleLz4Error [CUnsafe.block| size_t {

    LZ4F_preferences_t* lz4_preferences_ptr = $(LZ4F_preferences_t* prefsPtr);

    size_t err_or_headerSize = LZ4F_compressBegin($(LZ4F_cctx* ctx), $(char* headerBuf), $(size_t headerBufLen), lz4_preferences_ptr);
    return err_or_headerSize;
  } |]

  return headerSize


lz4fCompressBound :: (HasCallStack) => CSize -> ScopedLz4FramePreferencesPtr -> IO CSize
lz4fCompressBound srcSize (ScopedLz4FramePreferencesPtr prefsPtr) = do
  handleLz4Error [CUnsafe.block| size_t {
    size_t err_or_frame_size = LZ4F_compressBound($(size_t srcSize), $(LZ4F_preferences_t* prefsPtr));
    return err_or_frame_size;
  } |]


lz4fCompressUpdate :: (HasCallStack) => ScopedLz4FrameCompressionContext -> Ptr CChar -> CSize -> Ptr CChar -> CSize -> IO CSize
lz4fCompressUpdate (ScopedLz4FrameCompressionContext ctx) destBuf destBufLen srcBuf srcBufLen = do
  -- TODO allow passing in cOptPtr instead of NULL.
  written <- handleLz4Error [CUnsafe.block| size_t {
    size_t err_or_written = LZ4F_compressUpdate($(LZ4F_cctx* ctx), $(char* destBuf), $(size_t destBufLen), $(char* srcBuf), $(size_t srcBufLen), NULL);
    return err_or_written;
  } |]
  return written


lz4fCompressEnd :: (HasCallStack) => ScopedLz4FrameCompressionContext -> Ptr CChar -> CSize -> IO CSize
lz4fCompressEnd (ScopedLz4FrameCompressionContext ctx) footerBuf footerBufLen = do
  -- TODO allow passing in cOptPtr instead of NULL.
  footerWritten <- handleLz4Error [CUnsafe.block| size_t {
    size_t err_or_footerWritten = LZ4F_compressEnd($(LZ4F_cctx* ctx), $(char* footerBuf), $(size_t footerBufLen), NULL);
    return err_or_footerWritten;
  } |]
  return footerWritten


blockSizeBytes :: BlockSizeID -> Int
blockSizeBytes x = case x of
  -- The default one is one of the other ones, but we have to ask the C lib which one.
  LZ4F_default -> blockSizeBytes actualBlockSizeId
    -- Ugly hack alert: Due to LZ4F_getBlockSize() not being exported
    --   https://github.com/lz4/lz4/issues/608
    -- we cannot ask the API which value LZ4F_default actually is.
    -- As a workaround, figure it out via `LZ4F_compressBound`.
    --
    -- Using `unsafePerformIO` is safe here because the lz4 API promises
    -- for LZ4F_compressBound():
    --     Result is always the same for a srcSize and prefsPtr,
    --     so it can be trusted to size reusable buffers.
    where
      actualBlockSizeId =
        unsafePerformIO $ do
          let prefsFor i =
                let fi = (frameInfo lz4DefaultPreferences){ blockSizeID = i }
                in lz4DefaultPreferences{ frameInfo = fi }

          defaultBound <- withScopedLz4fPreferences (prefsFor LZ4F_default) $ \prefs -> do
            lz4fCompressBound 0 prefs

          idsWithBounds <- for (delete LZ4F_default [minBound..maxBound]) $ \i -> do
            withScopedLz4fPreferences (prefsFor i) $ \prefs -> do
              bound <- lz4fCompressBound 0 prefs
              return (i, bound)

          case [ i | (i, bound) <- idsWithBounds, bound == defaultBound ] of
            [i] -> return i
            _ -> error $ "blockSizeBytes: Got unexpected (idsWithBounds, defaultBound):" ++ show (idsWithBounds, defaultBound)
  LZ4F_max64KB -> 64 * 1024
  LZ4F_max256KB -> 256 * 1024
  LZ4F_max1MB -> 1 * 1024 * 1024
  LZ4F_max4MB -> 4 * 1024 * 1024


-- Note [Single call to LZ4F_compressUpdate() can create multiple blocks]
-- A single call to LZ4F_compressUpdate() can create multiple blocks,
-- and handles buffers > 32-bit sizes; see:
--   https://github.com/lz4/lz4/blob/52cac9a97342641315c76cfb861206d6acd631a8/lib/lz4frame.c#L601
-- So we don't need to loop around LZ4F_compressUpdate() to compress
-- an arbitrarily large amount of input data, as long as the destination
-- buffer is large enough.


compress :: (MonadUnliftIO m, MonadResource m) => ConduitT ByteString ByteString m ()
compress = compressWithOutBufferSize 0 lz4DefaultPreferences


withLz4PrefsConduit ::
  (MonadUnliftIO m, MonadResource m)
  => Preferences
  -> (ScopedLz4FramePreferencesPtr -> ConduitT i o m r)
  -> ConduitT i o m r
withLz4PrefsConduit prefs f = bracketP
  (do
    prefsPtr <- new prefs
    return (ScopedLz4FramePreferencesPtr prefsPtr)
  )
  (\(ScopedLz4FramePreferencesPtr prefsPtr) -> do
    free prefsPtr
  )
  f


withLz4CtxConduit ::
  (MonadUnliftIO m, MonadResource m)
  => (ScopedLz4FrameCompressionContext -> ConduitT i o m r)
  -> ConduitT i o m r
withLz4CtxConduit f =
  bracketP
    allocateLz4fScopedCompressionContext
    freeLz4ScopedCompressionContext
    f


-- | Compresses the incoming stream of ByteStrings with the lz4 frame format.
--
-- Turns every incoming `ByteString` into a full block using lz4's simple
-- API. The simple API enables lz4 frame autoFlush, which disables combining
-- multiple inputs into a single block. (This is opposed to the advanced API
-- that can combine multiple inputs into a single block; for that see the other
-- compression functions in this module).
--
-- This means every input fed into this conduit will immediately
-- result in exactly one ByteString being produced.
--
-- It is not recommended to use this function when the incoming `ByteString`s
-- are small, because each block carries a few bytes overhead.
--
-- This conduit allocates and maintains an output buffer into which lz4 writes
-- its data. The buffer is automatically grown as needed (and never shrunk),
-- but it is capped to a constant slightly larger than the @blockSizeID@ in
-- the given `Preferences`.
--
-- For large `ByteString`s of (roughly) equal size, this is faster than
-- the advanced API because the input is processed zero-copy fashion.
compressNoBuffering :: (MonadUnliftIO m, MonadResource m) => Preferences -> ConduitT ByteString ByteString m ()
compressNoBuffering haskellPrefs = do
  let dstBufferSizeDefault :: CSize
      dstBufferSizeDefault = 16 * 1024

  bracketP
    (do
      dstBufferPtr <- malloc
      dstBufferSizePtr <- malloc
      poke dstBufferPtr =<< mallocArray (fromIntegral dstBufferSizeDefault)
      poke dstBufferSizePtr dstBufferSizeDefault
      return (dstBufferPtr, dstBufferSizePtr)
    )
    (\(dstBufferPtr, dstBufferSizePtr) -> do
      free =<< peek dstBufferPtr
      free dstBufferPtr
      free dstBufferSizePtr
    )
    $ \(dstBufferPtr, dstBufferSizePtr) -> do

    let ensureDstBufferSize :: CSize -> IO (Ptr CChar)
        ensureDstBufferSize size = do
          dstBufferSize <- peek dstBufferSizePtr
          when (size > dstBufferSize) $ do
            dstBuffer <- peek dstBufferPtr
            poke dstBufferPtr =<< reallocArray dstBuffer (fromIntegral size)
            poke dstBufferSizePtr size
          peek dstBufferPtr

    withLz4PrefsConduit haskellPrefs $ \prefs -> do

      let ScopedLz4FramePreferencesPtr prefsPtr = prefs

      awaitForever $ \bs -> do

        outBs <- liftIO $ unsafeUseAsCStringLen bs $ \(srcBuf, srcBufLenInt) -> do

          let srcBufLen = fromIntegral srcBufLenInt

          destBufLen <- lz4fCompressFrameBound srcBufLen prefs
          destBuf <- ensureDstBufferSize destBufLen

          written <- handleLz4Error [CUnsafe.block| size_t {
            size_t err_or_written = LZ4F_compressFrame($(char* destBuf), $(size_t destBufLen), $(char* srcBuf), $(size_t srcBufLen), $(LZ4F_preferences_t* prefsPtr));
            return err_or_written;
          } |]
          packCStringLen (destBuf, fromIntegral written)

        yield outBs


-- | Compresses the incoming stream of ByteStrings with the lz4 frame format.
--
-- Yields every LZ4 output as a ByteString as soon as the lz4 frame
-- library produces it (so there is no output buffering to try to reduce
-- the number of outputs emitted for the case that inputs compress very well).
--
-- Note that this does not imply ZL4 frame autoFlush (see also `compressNoBuffering`),
-- so input buffering is still happening to combine small inputs.
compressYieldImmediately :: (MonadUnliftIO m, MonadResource m) => Preferences -> ConduitT ByteString ByteString m ()
compressYieldImmediately haskellPrefs =
  withLz4PrefsConduit haskellPrefs $ \prefs -> do
  withLz4CtxConduit $ \ctx -> do

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
        --      Worse, with autoflush=0, most small inputs go into the context input buffer,
        --      in which case the `allocaBytes` is completely wasted.
        --      This could be avoided by keeping the last `allocaBytes` buffer around,
        --      and reusing it if it is big enough for the number returned by
        --      LZ4F_compressUpdate() next time.
        --      That should avoid most allocations in the case that we `await` lots of
        --      small ByteStrings.
        m'outBs <- liftIO $ allocaBytes (fromIntegral size) $ \buf -> do

          -- See note [Single call to LZ4F_compressUpdate() can create multiple blocks].
          written <- lz4fCompressUpdate ctx buf size bsPtr bsLenSize

          if written == 0 -- everything fit into the context input buffer, no new compressed data was emitted
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
-- * The `bufferSize` of the output buffer can controlled by the caller
--   via the `bufferSize` argument, to reduce the number of small
--   `ByteString`s being `yield`ed (especially in the case that the
--   input data compresses very well, e.g. a stream of zeros).
--
-- Note that the given `bufferSize` is not a hard limit, it can only be
-- used to *increase* the amount of output buffer we're allowed to use:
-- The function will choose `max(bufferSize, minBufferSizeNeededByLz4)`
-- as the eventual output buffer size.
--
-- Setting `bufferSize = 0` is the legitimate way to set the output buffer
-- size to be the minimum required to compress 16 KB inputs and is still a
-- fast default.
compressWithOutBufferSize :: forall m . (MonadUnliftIO m, MonadResource m) => CSize -> Preferences -> ConduitT ByteString ByteString m ()
compressWithOutBufferSize bufferSize haskellPrefs =
  withLz4PrefsConduit haskellPrefs $ \prefs -> do
  withLz4CtxConduit $ \ctx -> do

    -- We split any incoming ByteString into chunks of this size, so that
    -- we can pass this size to `lz4fCompressBound` once and reuse a buffer
    -- of constant size for the compression.
    -- We choose this to be equal the output block size (via blockSizeID),
    -- because inputs being >= than the block size is the "fast path"
    -- in LZ4F_compressUpdate() that does not copy inputs into the
    -- context input buffer.
    let bsInChunkSize = fromIntegral $ blockSizeBytes (blockSizeID (frameInfo haskellPrefs))

    compressBound <- liftIO $ lz4fCompressBound bsInChunkSize prefs
    -- The compressBound already includes the size of the footer,
    -- see LZ4F_compressBound() docs.
    let outBufferSize = max bufferSize (#{const LZ4F_HEADER_SIZE_MAX} + compressBound)


    outBuf <- liftIO $ mallocForeignPtrBytes (fromIntegral outBufferSize)
    let withOutBuf f = liftIO $ withForeignPtr outBuf f
    let yieldOutBuf outBufLen = do
          outBs <- withOutBuf $ \buf -> packCStringLen (buf, fromIntegral outBufLen)
          yield outBs

    headerSize <- withOutBuf $ \buf -> lz4fCompressBegin ctx prefs buf outBufferSize

    let writeFooterAndYield remainingCapacity = do
          let offset = fromIntegral $ outBufferSize - remainingCapacity
          footerWritten <- withOutBuf $ \buf -> lz4fCompressEnd ctx (buf `plusPtr` offset) remainingCapacity
          let outBufLen = outBufferSize - remainingCapacity + footerWritten
          yieldOutBuf outBufLen

    let loop remainingCapacity = do
          await >>= \case
            Nothing -> do
              -- Done, write footer.
              -- We know it fits because it's already accounted for
              -- in lz4fCompressBound above.
              writeFooterAndYield remainingCapacity

            Just bs -> do
              let bss = bsChunksOf (fromIntegral bsInChunkSize) bs
              -- Invariant during the fold:
              -- There's always enough `remainingCapacity` to fit
              -- at least one incoming ByteString + the footer
              -- (that is, compressBound <= remainingCapacity)
              newRemainingCapacity <- foldM (\cap subBs -> compressFittingBs cap subBs) remainingCapacity bss
              loop newRemainingCapacity

        -- PRE: compressBound <= remainingCapacity
        compressFittingBs :: CSize -> ByteString -> ConduitM i ByteString m CSize
        compressFittingBs remainingCapacity bs = do

          written <- liftIO $ unsafeUseAsCStringLen bs $ \(bsPtr, bsLen) -> do
            let bsLenSize = fromIntegral bsLen

            -- See note [Single call to LZ4F_compressUpdate() can create multiple blocks]
            let offset = fromIntegral $ outBufferSize - remainingCapacity
            withOutBuf $ \buf -> lz4fCompressUpdate ctx (buf `plusPtr` offset) remainingCapacity bsPtr bsLenSize

          if
            | written == 0 -> return remainingCapacity
            | otherwise -> do
                let newRemainingCapacity = remainingCapacity - written
                if compressBound <= newRemainingCapacity
                  then return newRemainingCapacity
                  else do
                    -- Not enough space in outBuf to guarantee that the next call
                    -- to `lz4fCompressUpdate` will fit; so yield (a copy of) the
                    -- current outBuf, then it's all free again.
                    yieldOutBuf (outBufferSize - newRemainingCapacity)
                    return outBufferSize

    loop (outBufferSize - headerSize)


-- All notes that apply to `haskell_lz4_freeCompressionContext` apply
-- here as well.
C.verbatim [r|
void haskell_lz4_freeDecompressionContext(LZ4F_dctx** ctxPtr)
{
  LZ4F_dctx* ctx = *ctxPtr;
  if (ctx != NULL)
  {
    size_t err = LZ4F_freeDecompressionContext(ctx);
    if (LZ4F_isError(err))
    {
      fprintf(stderr, "LZ4F_freeDecompressionContext failed: %s\n", LZ4F_getErrorName(err));
      exit(1);
    }
  }
}
|]

foreign import ccall "&haskell_lz4_freeDecompressionContext" haskell_lz4_freeDecompressionContext :: FunPtr (Ptr (Ptr LZ4F_dctx) -> IO ())


lz4fCreateDecompressionContext :: (HasCallStack) => IO Lz4FrameDecompressionContext
lz4fCreateDecompressionContext = do
  -- All notes that apply to `lz4fCreateCompressonContext` apply here
  -- as well.
  ctxForeignPtr :: ForeignPtr (Ptr LZ4F_dctx) <- mallocForeignPtr
  withForeignPtr ctxForeignPtr $ \ptr -> poke ptr nullPtr

  addForeignPtrFinalizer haskell_lz4_freeDecompressionContext ctxForeignPtr

  _ <- handleLz4Error [CUnsafe.block| size_t {
    LZ4F_dctx** ctxPtr = $fptr-ptr:(LZ4F_dctx** ctxForeignPtr);
    LZ4F_errorCode_t err = LZ4F_createDecompressionContext(ctxPtr, LZ4F_VERSION);
    return err;
  } |]
  return (Lz4FrameDecompressionContext ctxForeignPtr)


lz4fGetFrameInfo :: (HasCallStack) => Lz4FrameDecompressionContext -> Ptr FrameInfo -> Ptr CChar -> Ptr CSize -> IO CSize
lz4fGetFrameInfo (Lz4FrameDecompressionContext ctxForeignPtr) frameInfoPtr srcBuffer srcSizePtr = do

  decompressSizeHint <- handleLz4Error [CUnsafe.block| size_t {
    LZ4F_dctx* ctxPtr = *$fptr-ptr:(LZ4F_dctx** ctxForeignPtr);
    LZ4F_errorCode_t err_or_decompressSizeHint = LZ4F_getFrameInfo(ctxPtr, $(LZ4F_frameInfo_t* frameInfoPtr), $(char* srcBuffer), $(size_t* srcSizePtr));
    return err_or_decompressSizeHint;
  } |]
  return decompressSizeHint


lz4fDecompress :: (HasCallStack) => Lz4FrameDecompressionContext -> Ptr CChar -> Ptr CSize -> Ptr CChar -> Ptr CSize -> IO CSize
lz4fDecompress (Lz4FrameDecompressionContext ctxForeignPtr) dstBuffer dstSizePtr srcBuffer srcSizePtr = do
  -- TODO allow passing in dOptPtr instead of NULL.

  decompressSizeHint <- handleLz4Error [CUnsafe.block| size_t {
    LZ4F_dctx* ctxPtr = *$fptr-ptr:(LZ4F_dctx** ctxForeignPtr);
    LZ4F_errorCode_t err_or_decompressSizeHint = LZ4F_decompress(ctxPtr, $(char* dstBuffer), $(size_t* dstSizePtr), $(char* srcBuffer), $(size_t* srcSizePtr), NULL);
    return err_or_decompressSizeHint;
  } |]
  return decompressSizeHint


decompress :: (MonadUnliftIO m, MonadResource m) => ConduitT ByteString ByteString m ()
decompress = do
  ctx <- liftIO lz4fCreateDecompressionContext

  -- The current code complies with the LZ4 Frame Format Description
  -- v1.6.0.

  -- OK, now here it gets a bit ugly.
  -- The lz4frame library provides no function with which we can
  -- determine how large the header is.
  -- It depends on the "Content Size" bit in the "FLG" Byte (first
  -- Byte of the frame descriptor, just after the 4 Byte magic number).
  -- As a solution, we `await` the first 5 Bytes, look at the relevant
  -- bit in the FLG Byte and thus decide how many more bytes to await
  -- for the header.
  -- Is ugly because ideally we would rely only on the lz4frame API
  -- and not on the spec of the frame format, but we have no other
  -- choice in this case.
  -- I filed https://github.com/lz4/lz4/issues/607 for it.

  first5Bytes <- CB.take 5
  when (BSL.length first5Bytes /= 5) $ do
    throwString $ "lz4 decompress error: not enough bytes for header; expected 5, got " ++ show (BSL.length first5Bytes)

  let byteFLG = BSL.index first5Bytes 4
  let contentSizeBit = testBit byteFLG 3
  let dictIDBit = testBit byteFLG 0

  let contentSizeHeaderBytes
        | contentSizeBit = 8
        | otherwise      = 0

  -- Available with LZ4 Frame Format Description spec >= v1.6.0.
  let dictionaryIDHeaderBytes
        | dictIDBit = 4
        | otherwise = 0

  let numRemainingHeaderBytes =
        1 -- BD byte
        + contentSizeHeaderBytes
        + dictionaryIDHeaderBytes
        + 1 -- HC byte

  remainingHeaderBytes <- CB.take numRemainingHeaderBytes

  let headerBs = BSL.toStrict $ BSL.concat [first5Bytes, remainingHeaderBytes]

  (info, headerDecompressSizeHint) <- liftIO $ alloca $ \frameInfoPtr -> do
    unsafeUseAsCStringLen headerBs $ \(headerBsPtr, headerBsLen) -> do
      with (fromIntegral headerBsLen :: CSize) $ \headerBsLenPtr -> do
        hint <- lz4fGetFrameInfo ctx frameInfoPtr headerBsPtr headerBsLenPtr
        info <- peek frameInfoPtr
        return (info, hint)

  -- Inform buffer size the BlockSizeID in the header.
  let dstBufferSizeDefault :: CSize
      dstBufferSizeDefault = fromIntegral $ blockSizeBytes (blockSizeID info)

  bracketP
    (do
      dstBufferPtr <- malloc
      dstBufferSizePtr <- malloc
      poke dstBufferPtr =<< mallocArray (fromIntegral dstBufferSizeDefault)
      poke dstBufferSizePtr dstBufferSizeDefault
      return (dstBufferPtr, dstBufferSizePtr)
    )
    (\(dstBufferPtr, dstBufferSizePtr) -> do
      free =<< peek dstBufferPtr
      free dstBufferPtr
      free dstBufferSizePtr
    )
    $ \(dstBufferPtr, dstBufferSizePtr) -> do

    let ensureDstBufferSize :: CSize -> IO (Ptr CChar)
        ensureDstBufferSize size = do
          dstBufferSize <- peek dstBufferSizePtr
          when (size > dstBufferSize) $ do
            dstBuffer <- peek dstBufferPtr
            poke dstBufferPtr =<< reallocArray dstBuffer (fromIntegral size)
            poke dstBufferSizePtr size
          peek dstBufferPtr

    let loopSingleBs :: CSize -> ByteString -> _
        loopSingleBs decompressSizeHint bs = do
          (outBs, srcRead, newDecompressSizeHint) <- liftIO $
            unsafeUseAsCStringLen bs $ \(srcBuffer, srcSize) -> do
              let outBufSize = max decompressSizeHint dstBufferSizeDefault

              -- Increase destination buffer size if necessary.
              dstBuffer <- ensureDstBufferSize outBufSize

              with outBufSize $ \dstSizePtr -> do
                with (fromIntegral srcSize :: CSize) $ \srcSizePtr -> do
                  newDecompressSizeHint <-
                    lz4fDecompress ctx dstBuffer dstSizePtr srcBuffer srcSizePtr
                  srcRead <- peek srcSizePtr
                  dstWritten <- peek dstSizePtr
                  outBs <- packCStringLen (dstBuffer, fromIntegral dstWritten)
                  return (outBs, srcRead, newDecompressSizeHint)

          yield outBs

          let srcReadInt = fromIntegral srcRead
          if
            | srcReadInt < BS.length bs -> loopSingleBs newDecompressSizeHint (BS.drop srcReadInt bs)
            | srcReadInt == BS.length bs -> return newDecompressSizeHint
            | otherwise -> error $ "lz4 decompress: assertion failed: srcRead < BS.length bs: " ++ show (srcRead, BS.length bs)

    let loop decompressSizeHint =
          await >>= \case
            Nothing -> throwString $ "lz4 decompress error: stream ended before EndMark"
            Just bs -> do
              newDecompressSizeHint <- loopSingleBs decompressSizeHint bs

              -- When a frame is fully decoded, LZ4F_decompress returns 0 (no more data expected),
              -- see https://github.com/lz4/lz4/blob/7cf0bb97b2a988cb17435780d19e145147dd9f70/lib/lz4frame.h#L324
              when (newDecompressSizeHint /= 0) $ loop newDecompressSizeHint

    loop headerDecompressSizeHint

    -- Force resource release here to guarantee memory constantness
    -- of the conduit (and not rely on GC to do it "at some point in the future").
    liftIO $ finalizeForeignPtr (unLz4FrameDecompressionContext ctx)
