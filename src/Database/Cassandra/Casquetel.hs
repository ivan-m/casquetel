{-# LANGUAGE DefaultSignatures, DeriveAnyClass, DeriveGeneric, FlexibleContexts,
             KindSignatures, TypeOperators #-}
{- |
   Module      : Database.Cassandra.Casquetel
   Description : Cassandra CQL with added safety
   Copyright   : (c) Ivan Lazar Miljenovic
   License     : MIT
   Maintainer  : Ivan.Miljenovic@gmail.com



 -}
module Database.Cassandra.Casquetel where

import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.Int
import           Data.UUID
import           Data.Word

import           Data.Bits
import           Data.Bool               (bool)
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as B
import           Data.Monoid             (mconcat, (<>))
import           GHC.Generics

--------------------------------------------------------------------------------

class ToCQL v where
  toCQL :: v -> Builder
  default toCQL :: (Generic v, GToCQL (Rep v)) => v -> Builder
  toCQL = gToCQL . from

  toCQLList :: [v] -> Builder
  toCQLList = mconcat . map toCQL

instance (ToCQL a, ToCQL b) => ToCQL (a,b)

instance (ToCQL v) => ToCQL [v] where
  toCQL = toCQLList

instance ToCQL Int8 where
  toCQL = B.int8

instance ToCQL Int16 where
  toCQL = B.int16BE

instance ToCQL Int32 where
  toCQL = B.int32BE

instance ToCQL Int64 where
  toCQL = B.int64BE

instance ToCQL Word8 where
  toCQL = B.word8

instance ToCQL Word16 where
  toCQL = B.word16BE

instance ToCQL Word32 where
  toCQL = B.word32BE

instance ToCQL Word64 where
  toCQL = B.word64BE

instance ToCQL ByteString where
  toCQL = B.byteString

maxBitsByte :: Int
maxBitsByte = finiteBitSize (0 :: Word8) -  1

--------------------------------------------------------------------------------

class GToCQL (f :: * -> *) where
  gToCQL :: f a -> Builder

-- Single value
instance (ToCQL v) => GToCQL (K1 i v) where
  gToCQL = toCQL . unK1

-- Meta information
instance (GToCQL f) => GToCQL (M1 i t f) where
  gToCQL = gToCQL . unM1

-- Product type
instance (GToCQL f, GToCQL g) => GToCQL (f :*: g) where
  gToCQL (a :*: b) = gToCQL a <> gToCQL b

--------------------------------------------------------------------------------

-- From https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v3.spec

-- * Section 1: Overview

data Frame = Frame { version     :: !FrameVersion
                   , flags       :: !FrameFlags
                   , streamID    :: !StreamID
                   , opCode      :: !OpCode
                   , frameLength :: !FrameLength
                   , frameBody   :: !ByteString -- TODO: this isn't right.
                   }
  deriving (Eq, Ord, Show, Read, Generic, ToCQL)

-- * Section 2: Frame header

-- ** Section 2.1: version

data FrameVersion = FrameVersion { frameDirection  :: !FrameDirection
                                 , protocolVersion :: !CByte  -- TODO: fix this
                                 }
  deriving (Eq, Ord, Show, Read, Generic)

instance ToCQL FrameVersion where
  toCQL (FrameVersion dir ver) = toCQL (directionValue dir .|. ver)

data FrameDirection = FrameRequest
                    | FrameResponse
  deriving (Eq, Ord, Show, Read, Generic)

directionValue :: FrameDirection -> CByte
directionValue FrameRequest  = 0
directionValue FrameResponse = bit maxBitsByte

-- ** Section 2.2: flags

data FrameFlags = FrameFlags { hasCompression :: !Bool
                             , hasTracing     :: !Bool
                             }
  deriving (Eq, Ord, Show, Read, Generic)

instance ToCQL FrameFlags where
  toCQL ff = toCQL (withMask 0x01 hasCompression .|. withMask 0x02 hasTracing)
    where
      withMask :: CByte -> (FrameFlags -> Bool) -> CByte
      withMask m v = bool 0 m (v ff)

-- ** Section 2.3: stream

type StreamID = CShort

-- ** Section 2.4: opcode

data OpCode = ERROR
            | STARTUP
            | READY
            | AUTHENTICATE
            | OPTIONS
            | SUPPORTED
            | QUERY
            | RESULT
            | PREPARE
            | EXECUTE
            | REGISTER
            | EVENT
            | BATCH
            | AUTH_CHALLENGE
            | AUTH_RESPONSE
            | AUTH_SUCCESS
  deriving (Eq, Ord, Show, Read, Generic)

instance ToCQL OpCode where
  toCQL = toCQL . opCodeValue

opCodeValue :: OpCode -> CByte
opCodeValue ERROR          = 0x00
opCodeValue STARTUP        = 0x01
opCodeValue READY          = 0x02
opCodeValue AUTHENTICATE   = 0x03
opCodeValue OPTIONS        = 0x05
opCodeValue SUPPORTED      = 0x06
opCodeValue QUERY          = 0x07
opCodeValue RESULT         = 0x08
opCodeValue PREPARE        = 0x09
opCodeValue EXECUTE        = 0x0A
opCodeValue REGISTER       = 0x0B
opCodeValue EVENT          = 0x0C
opCodeValue BATCH          = 0x0D
opCodeValue AUTH_CHALLENGE = 0x0E
opCodeValue AUTH_RESPONSE  = 0x0F
opCodeValue AUTH_SUCCESS   = 0x10

-- ** Section 2.5: length

type FrameLength = Word32 -- Guessing it's unsigned

-- * Section 3: Notations

type CInt = Int32
type CLong = Int64
type CShort = Word16
type CByte = Word8

data CString = CString { csLen   :: !CShort
                       , csBytes :: !ByteString
                         -- ^ Must have length of csLen, and
                         -- correspond to a UTF-8 string.
                       }
  deriving (Eq, Ord, Show, Read, Generic, ToCQL)

data CLongString = CLongString { clsLen   :: !CShort
                               , clsBytes :: !ByteString
                                 -- ^ Must have length of clsLen, and
                                 -- correspond to a UTF-8 string.
                               }
  deriving (Eq, Ord, Show, Read, Generic, ToCQL)

type CUUID = UUID

data CStringList = CStringList { cslLen  :: !CShort
                               , cslList :: ![CString]
                                 -- ^ Must have length of cslLen
                               }
  deriving (Eq, Ord, Show, Read, Generic, ToCQL)

data COption a = COption { coID    :: !CShort
                         , coValue :: !a
                         }
  deriving (Eq, Ord, Show, Read, Generic, ToCQL)

data COptionList a = COptionList { colLen  :: !CShort
                                 , colList :: ![COption a]
                                   -- ^ Must have length of colLen
                                 }
  deriving (Eq, Ord, Show, Read, Generic, ToCQL)

data CInet = CInet { ciSize    :: !CByte
                   , ciAddress :: !ByteString
                     -- ^ Must have length ciSize
                   , ciPort    :: !CInt
                   }
  deriving (Eq, Ord, Show, Read, Generic, ToCQL)

data CConsistency = ANY
                  | ONE
                  | TWO
                  | THREE
                  | QUORUM
                  | ALL
                  | LOCAL_QUORUM
                  | EACH_QUORUM
                  | SERIAL
                  | LOCAL_SERIAL
                  | LOCAL_ONE
  deriving (Eq, Ord, Show, Read, Generic)

instance ToCQL CConsistency where
  toCQL = toCQL . consistencyValue

consistencyValue :: CConsistency -> CShort
consistencyValue ANY          = 0x0000
consistencyValue ONE          = 0x0001
consistencyValue TWO          = 0x0002
consistencyValue THREE        = 0x0003
consistencyValue QUORUM       = 0x0004
consistencyValue ALL          = 0x0005
consistencyValue LOCAL_QUORUM = 0x0006
consistencyValue EACH_QUORUM  = 0x0007
consistencyValue SERIAL       = 0x0008
consistencyValue LOCAL_SERIAL = 0x0009
consistencyValue LOCAL_ONE    = 0x000A

data CStringMap = CStringMap { csmSize :: !CShort
                             , csmMap  :: ![(CString, CString)]
                               -- ^ Must have length csmSize
                               --
                               -- TODO: consider using a Map
                             }
  deriving (Eq, Ord, Show, Read, Generic, ToCQL)

data CStringMultiMap = CStringMultiMap { csmmSize :: !CShort
                                       , csmmMap  :: ![(CString, CStringList)]
                                         -- ^ Must have length csmmSize
                                         --
                                         -- TODO: consider using a Map
                                       }
  deriving (Eq, Ord, Show, Read, Generic, ToCQL)
