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
  ) where

import           Control.Exception.Safe (MonadThrow, throwString)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Data.ByteString (ByteString, packCStringLen)
import           Data.ByteString.Unsafe (unsafePackCString, unsafeUseAsCStringLen)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import           Data.Conduit
import           Data.Monoid ((<>))
import           Foreign.C.Types (CChar, CSize)
import           Foreign.ForeignPtr (ForeignPtr, addForeignPtrFinalizer, mallocForeignPtr, finalizeForeignPtr, withForeignPtr)
import           Foreign.Marshal.Alloc (allocaBytes)
import           Foreign.Ptr (Ptr, nullPtr, FunPtr)
import           Foreign.Storable (Storable(..), poke)
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


handleLz4Error :: (MonadIO m, MonadThrow m) => IO CSize -> m CSize
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
      throwString (BS8.unpack errMsgBs)


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


lz4fCreateContext :: IO Lz4FrameContext
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


lz4fCompressBegin :: Lz4FrameContext -> Lz4FramePreferencesPtr -> Ptr CChar -> CSize -> IO CSize
lz4fCompressBegin (Lz4FrameContext ctxForeignPtr) (Lz4FramePreferencesPtr prefsForeignPtr) headerBuf headerBufLen = do
  headerSize <- handleLz4Error [C.block| size_t {

    LZ4F_cctx* ctx = *$fptr-ptr:(LZ4F_cctx** ctxForeignPtr);
    LZ4F_preferences_t* lz4_preferences_ptr = $fptr-ptr:(LZ4F_preferences_t* prefsForeignPtr);

    size_t err_or_headerSize = LZ4F_compressBegin(ctx, $(char* headerBuf), $(size_t headerBufLen), lz4_preferences_ptr);
    return err_or_headerSize;
  } |]

  return headerSize


lz4fCompressBound :: CSize -> Lz4FramePreferencesPtr -> IO CSize
lz4fCompressBound srcSize (Lz4FramePreferencesPtr prefsForeignPtr) = do
  handleLz4Error [C.block| size_t {
    size_t err_or_frame_size = LZ4F_compressBound($(size_t srcSize), $fptr-ptr:(LZ4F_preferences_t* prefsForeignPtr));
    return err_or_frame_size;
  } |]


lz4fCompressUpdate :: Lz4FrameContext -> Ptr CChar -> CSize -> Ptr CChar -> CSize -> IO CSize
lz4fCompressUpdate (Lz4FrameContext ctxForeignPtr) destBuf destBufLen srcBuf srcBufLen = do
  -- TODO allow passing in cOptPtr instead of NULL.
  written <- handleLz4Error [C.block| size_t {

    LZ4F_cctx* ctx = *$fptr-ptr:(LZ4F_cctx** ctxForeignPtr);

    size_t err_or_written = LZ4F_compressUpdate(ctx, $(char* destBuf), $(size_t destBufLen), $(char* srcBuf), $(size_t srcBufLen), NULL);
    return err_or_written;
  } |]
  return written


lz4fCompressEnd :: Lz4FrameContext -> Ptr CChar -> CSize -> IO CSize
lz4fCompressEnd (Lz4FrameContext ctxForeignPtr) footerBuf footerBufLen = do
  -- TODO allow passing in cOptPtr instead of NULL.
  footerWritten <- handleLz4Error [C.block| size_t {
    LZ4F_cctx* ctx = *$fptr-ptr:(LZ4F_cctx** ctxForeignPtr);

    size_t err_or_footerWritten = LZ4F_compressEnd(ctx, $(char* footerBuf), $(size_t footerBufLen), NULL);
    return err_or_footerWritten;
  } |]
  return footerWritten


compress :: (MonadIO m, MonadThrow m) => Conduit ByteString m ByteString
compress = do
  ctx <- liftIO lz4fCreateContext
  prefs <- liftIO lz4fCreatePreferences

  let _LZ4F_HEADER_SIZE_MAX = #{const LZ4F_HEADER_SIZE_MAX}

  -- Header

  headerBs <- liftIO $ allocaBytes (fromIntegral _LZ4F_HEADER_SIZE_MAX) $ \headerBuf -> do

    headerSize <- lz4fCompressBegin ctx prefs headerBuf _LZ4F_HEADER_SIZE_MAX
    liftIO $ print ("headerSize written", headerSize)
    packCStringLen (headerBuf, fromIntegral headerSize)

  yield headerBs

  -- Chunks

  awaitForever $ \bs -> do

    liftIO $ print ("bs length", BS.length bs)

    m'outBs <- liftIO $ unsafeUseAsCStringLen bs $ \(bsPtr, bsLen) -> do
      let bsLenSize = fromIntegral bsLen
      liftIO $ print ("bsLenSize", bsLenSize)

      size <- lz4fCompressBound bsLenSize prefs
      liftIO $ print ("size", size)

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

        -- Note: A single call to LZ4F_compressUpdate() can create multiple blocks,
        -- and handles buffers > 32-bit sizes; see:
        --   https://github.com/lz4/lz4/blob/52cac9a97342641315c76cfb861206d6acd631a8/lib/lz4frame.c#L601
        -- So we don't need to loop here.
        -- Also note that
        written <- lz4fCompressUpdate ctx buf size bsPtr bsLenSize
        print ("written", written)

        if written == 0 -- everything fit into the context buffer, no new compressed data was emitted
          then return Nothing
          else Just <$> packCStringLen (buf, fromIntegral written)

      return m'outBs

    case m'outBs of
      Nothing -> return ()
      Just outBs -> do
        liftIO $ print ("length outBs", BS.length outBs)
        yield outBs

  -- Footer

  -- Passing srcSize==0 provides bound for LZ4F_compressEnd(),
  -- see docs of LZ4F_compressBound() for that.
  footerSize <- liftIO $ lz4fCompressBound 0 prefs

  footerBs <- liftIO $ allocaBytes (fromIntegral footerSize) $ \footerBuf -> do

    footerWritten <- lz4fCompressEnd ctx footerBuf footerSize
    print ("footer footerWritten", footerWritten)

    packCStringLen (footerBuf, fromIntegral footerWritten)

  yield footerBs

  -- Force resource release here to guarantee memory constantness
  -- of the conduit (and not rely on GC to do it "at some point in the future").
  liftIO $ finalizeForeignPtr (unLz4FramePreferencesPtr prefs)
  liftIO $ finalizeForeignPtr (unLz4FrameContext ctx)

  liftIO $ putStrLn "done"
