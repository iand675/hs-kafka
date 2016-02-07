{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}
module Network.Kafka.Types where
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import           Data.Digest.CRC32
import           Data.Int
import qualified Data.Vector as V
import qualified Data.Vector.Generic as G
import           GHC.Generics hiding (to)
import           GHC.TypeLits
import           Network.Kafka.Exports
import           Network.Kafka.Fields

import           Debug.Trace

newtype Array v a = Array { fromArray :: v a }
  deriving (Show, Eq, Generic)

instance (Binary a, G.Vector v a) => Binary (Array v a) where
  get = do
    len <- fromIntegral <$> getWord32be
    Array <$> G.replicateM len get
  put (Array v) = do
    putWord32be $ fromIntegral $ G.length v
    G.mapM_ put v

instance (ByteSize a, Functor f, Foldable f) => ByteSize (Array f a) where
  byteSize f = 4 + sum (byteSize <$> fromArray f)

instance ByteSize a => ByteSize (V.Vector a) where
  byteSize v = 4 + G.sum (G.map byteSize v)

newtype FixedArray v a = FixedArray { fromFixedArray :: v a }
  deriving (Show, Eq, Generic)

instance (Binary a, G.Vector v a) => Binary (FixedArray v a) where
  get = (FixedArray . fromArray) <$> get
  put = put . Array . fromFixedArray
  
instance (ByteSize a, G.Vector v a) => ByteSize (FixedArray v a) where
  byteSize (FixedArray v) = 4 + (fromIntegral (G.length v) * singleSize v undefined)
    where
      singleSize :: ByteSize b => f b -> b -> Int32
      singleSize _ = byteSize


data ErrorCode
  = NoError
  | Unknown
  | OffsetOutOfRange
  | InvalidMessage
  | UnknownTopicOrPartition
  | InvalidMessageSize
  | LeaderNotAvailable
  | NotLeaderForPartition
  | RequestTimedOut
  | BrokerNotAvailable
  | ReplicaNotAvailable
  | MessageSizeTooLarge
  | StaleControllerEpoch
  | OffsetMetadataTooLarge
  | OffsetsLoadInProgress
  | ConsumerCoordinatorNotAvailable
  | NotCoordinatorForConsumer
  deriving (Show, Eq, Generic)


instance Enum ErrorCode where
  toEnum c = case c of
    0 -> NoError
    1 -> OffsetOutOfRange
    2 -> InvalidMessage
    3 -> UnknownTopicOrPartition
    4 -> InvalidMessageSize
    5 -> LeaderNotAvailable
    6 -> NotLeaderForPartition
    7 -> RequestTimedOut
    8 -> BrokerNotAvailable
    9 -> ReplicaNotAvailable
    10 -> MessageSizeTooLarge
    11 -> StaleControllerEpoch
    12 -> OffsetMetadataTooLarge
    14 -> OffsetsLoadInProgress
    15 -> ConsumerCoordinatorNotAvailable
    16 -> NotCoordinatorForConsumer
    _ -> Unknown
  fromEnum c = case c of
    NoError -> 0
    Unknown -> -1
    OffsetOutOfRange -> 1
    InvalidMessage -> 2
    UnknownTopicOrPartition -> 3
    InvalidMessageSize -> 4
    LeaderNotAvailable -> 5
    NotLeaderForPartition -> 6
    RequestTimedOut -> 7
    BrokerNotAvailable -> 8
    ReplicaNotAvailable -> 9
    MessageSizeTooLarge -> 10
    StaleControllerEpoch -> 11
    OffsetMetadataTooLarge -> 12
    OffsetsLoadInProgress -> 14
    ConsumerCoordinatorNotAvailable -> 15
    NotCoordinatorForConsumer -> 16

instance Binary ErrorCode where
  get = (toEnum . fromIntegral) <$> (get :: Get Int16)
  put = (\x -> put (x :: Int16)) . fromIntegral . fromEnum

instance ByteSize ErrorCode where
  byteSize = const 2


newtype ApiKey = ApiKey { fromApiKey :: Int16 }
  deriving (Show, Eq, Ord, Binary, ByteSize)


newtype ApiVersion = ApiVersion { fromApiVersion :: Int16 }
  deriving (Show, Eq, Ord, Binary, ByteSize)


newtype CorrelationId = CorrelationId { fromCorrelationId :: Int32 }
  deriving (Show, Eq, Ord, Binary, ByteSize)


newtype CoordinatorId = CoordinatorId { fromCoordinatorId :: Int32 }
  deriving (Show, Eq, Ord, Binary, ByteSize)


newtype NodeId = NodeId { fromNodeId :: Int32 }
  deriving (Show, Eq, Ord, Binary, ByteSize)


newtype PartitionId = PartitionId { fromPartitionId :: Int32 }
  deriving (Show, Eq, Ord, Binary, ByteSize)

newtype ConsumerId = ConsumerId { fromConsumerId :: Utf8 }
  deriving (Show, Eq, Binary, ByteSize)


newtype Utf8 = Utf8 { fromUtf8 :: ByteString }
  deriving (Show, Eq)


instance Binary Utf8 where
  get = do
    len <- fromIntegral <$> getWord16be
    Utf8 <$> getByteString len
  put (Utf8 str) = do
    putWord16be $ fromIntegral $ BS.length str
    putByteString str

instance ByteSize Utf8 where
  byteSize (Utf8 bs) = 2 + fromIntegral (BS.length bs)

newtype Bytes = Bytes { fromBytes :: ByteString }
  deriving (Show, Eq)

instance Binary Bytes where
  get = do
    len <- fromIntegral <$> getWord32be
    Bytes <$> getByteString len
  put (Bytes bs) = do
    putWord32be $ fromIntegral $ BS.length bs
    putByteString bs

instance ByteSize Bytes where
  byteSize (Bytes bs) = 4 + fromIntegral (BS.length bs)

data CompressionCodec = NoCompression
                      | GZip
                      | Snappy
                      deriving (Show, Eq)


newtype Attributes = Attributes { attributesCompression :: CompressionCodec }
  deriving (Show, Eq)

instance ByteSize Attributes where
  byteSize = const 1

instance Binary Attributes where
  get = do
    x <- get :: Get Int8
    return $ Attributes $! case x of
      0 -> NoCompression
      1 -> GZip
      2 -> Snappy
      _ -> error "Unsupported compression codec"
  put (Attributes c) = do
    let x = case c of
          NoCompression -> 0
          GZip          -> 1
          Snappy        -> 2
    put (x :: Int8)

data family RequestMessage p (v :: Nat)
data family ResponseMessage p (v :: Nat)

data ConsumerMetadata
data Produce
data Fetch
data Metadata
data Offset
data OffsetCommit
data OffsetFetch

data Message = Message
  -- messageCrc would go here
  { messageMagicByte  :: !Int8
  , messageAttributes :: !Attributes
  , messageKey        :: !Bytes
  , messageValue      :: !Bytes
  } deriving (Show, Eq, Generic)

instance ByteSize Message where
  byteSize m = 4 + -- crc
               byteSizeL magicByte m +
               byteSizeL attributes m +
               byteSize (messageKey m) +
               byteSize (messageValue m)
  {-# INLINE byteSize #-}

instance Binary Message where
  get = do
    crc <- get :: Get Word32
    bsStart <- bytesRead
    (msg, bsEnd) <- lookAhead $ do
      msg <- Message <$> get <*> get <*> get <*> get
      bsEnd <- bytesRead
      return (msg, bsEnd)
    crcChunk <- getByteString $ fromIntegral (bsEnd - bsStart)
    let crcFinal = crc32 crcChunk
    if crcFinal /= crc
      then fail ("Invalid CRC: expected " ++ show crc ++ ", got " ++ show crcFinal)
      else return msg
  put p = do
    let rest = runPut $ do
          putL magicByte p
          putL attributes p
          put $ messageKey p
          put $ messageValue p
        msgCrc = crc32 rest
    put msgCrc
    putLazyByteString rest

instance HasMagicByte Message Int8 where
  magicByte = lens messageMagicByte (\s a -> s { messageMagicByte = a })
  {-# INLINEABLE magicByte #-}

instance HasAttributes Message Attributes where
  attributes = lens messageAttributes (\s a -> s { messageAttributes = a })
  {-# INLINEABLE attributes #-}

instance HasKey Message ByteString where
  key = lens (fromBytes . messageKey) (\s a -> s { messageKey = Bytes a })
  {-# INLINEABLE key #-}

instance HasValue Message ByteString where
  value = lens (fromBytes . messageValue) (\s a -> s { messageValue = Bytes a })
  {-# INLINEABLE value #-}

data MessageSetItem = MessageSetItem
  { messageSetItemOffset  :: !Int64
  , messageSetItemMessage :: !Message
  } deriving (Show, Eq, Generic)

instance Binary MessageSetItem where
  get = do
    o <- get
    s <- get :: Get Int32
    m <- isolate (fromIntegral s) get
    return $ MessageSetItem o m
  {-# INLINE get #-}
  put i = do
    putL offset i
    putL (message . to byteSize) i
    putL message i
  {-# INLINE put #-}

instance ByteSize MessageSetItem where
  byteSize m = byteSizeL offset m +
               4 + -- MessageSize
               byteSizeL message m
  {-# INLINE byteSize #-}

instance HasOffset MessageSetItem Int64 where
  offset = lens messageSetItemOffset (\s a -> s { messageSetItemOffset = a })
  {-# INLINEABLE offset #-}

instance HasMessage MessageSetItem Message where
  message = lens messageSetItemMessage (\s a -> s { messageSetItemMessage = a })
  {-# INLINEABLE message #-}

newtype MessageSet = MessageSet
  { messageSetMessages :: [MessageSetItem]
  } deriving (Show, Eq)

instance ByteSize MessageSet where
  byteSize = sum . map byteSize . messageSetMessages

getMessageSet :: Int32 -> Get MessageSet
getMessageSet c = fmap (MessageSet . reverse) $ isolate (fromIntegral c) $ go []
  where
    go xs = do
      pred <- isEmpty
      if pred
        then return xs
        else do
          x <- get
          go (x:xs)

putMessageSet :: MessageSet -> Put
putMessageSet = mapM_ put . messageSetMessages


newtype GenerationId = GenerationId Int32
  deriving (Eq, Show, Binary, ByteSize)

newtype RetentionTime = RetentionTime Int64
  deriving (Eq, Show, Binary, ByteSize)

putL :: Binary a => Getter s a -> s -> Put
putL l = put . view l

data Request a v = Request
  { requestCorrelationId :: !CorrelationId
  , requestClientId      :: !Utf8
  , requestMessage       :: !(RequestMessage a v)
  } deriving (Generic)

deriving instance (Show (RequestMessage a v)) => Show (Request a v)
deriving instance (Eq (RequestMessage a v)) => Eq (Request a v)

instance (KnownNat v, RequestApiKey a, ByteSize (RequestMessage a v), Binary (RequestMessage a v)) => Binary (Request a v) where
  get = do
    _ <- get :: Get ApiKey
    _ <- get :: Get ApiVersion
    Request <$> get <*> get <*> get
  put p = do
    put $ apiKey p
    put $ ApiVersion $ fromIntegral $ natVal p
    put $ requestCorrelationId p
    put $ requestClientId p
    put $ requestMessage p


theApiKey :: Int16 -> a -> ApiKey
theApiKey x = const $ ApiKey x

class RequestApiKey p where
  apiKey :: Request p v -> ApiKey

instance RequestApiKey Produce where
  apiKey = theApiKey 0

instance RequestApiKey Fetch where
  apiKey = theApiKey 1

instance RequestApiKey Offset where
  apiKey = theApiKey 2

instance RequestApiKey Metadata where
  apiKey = theApiKey 3

instance RequestApiKey OffsetCommit where
  apiKey = theApiKey 8

instance RequestApiKey OffsetFetch where
  apiKey = theApiKey 9

instance RequestApiKey ConsumerMetadata where
  apiKey = theApiKey 10

data Response v a = Response
  { responseCorrelationId :: !CorrelationId
  , responseMessage       :: !(ResponseMessage v a)
  } deriving (Generic)

instance (KnownNat v, RequestApiKey a, ByteSize (ResponseMessage a v), Binary (ResponseMessage a v)) => Binary (Response a v) where
  get = Response <$> get <*> get
  put p = do
    put $ responseCorrelationId p
    put $ responseMessage p

deriving instance (Show (ResponseMessage a v)) => Show (Response a v)
deriving instance (Eq (ResponseMessage a v)) => Eq (Response a v)

instance (ByteSize (ResponseMessage a v)) => ByteSize (Response a v) where
  byteSize p = byteSize (responseCorrelationId p) +
               byteSize (responseMessage p)

instance (KnownNat v, ByteSize (RequestMessage a v)) => ByteSize (Request a v) where
  byteSize p = byteSize (undefined :: ApiKey) +
               byteSize (ApiVersion $ fromIntegral $ natVal p) +
               byteSize (requestCorrelationId p) +
               byteSize (requestClientId p) +
               byteSize (requestMessage p)

