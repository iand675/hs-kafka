{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
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
import           Data.Int
import qualified Data.Vector as V
import qualified Data.Vector.Generic as G
import           GHC.Generics
import           GHC.TypeLits
import           Network.Kafka.Exports
import           Network.Kafka.Fields

import           Debug.Trace

newtype Array v a = Array { fromArray :: v a }
  deriving (Show, Generic)

instance (Binary a, G.Vector v a) => Binary (Array v a) where
  get = do
    len <- fromIntegral <$> getWord32be
    Array <$> G.replicateM len get
  put (Array v) = do
    putWord32be $ fromIntegral $ G.length v
    G.mapM_ put v

instance ByteSize a => ByteSize (V.Vector a) where
  byteSize v = 4 + (G.sum $ G.map byteSize v)

newtype FixedArray v a = FixedArray { fromFixedArray :: v a }
  deriving (Show, Generic)

instance (Binary a, G.Vector v a) => Binary (FixedArray v a) where
  get = (FixedArray . fromArray) <$> get
  put = put . Array . fromFixedArray
  
instance (ByteSize a, G.Vector v a) => ByteSize (FixedArray v a) where
  byteSize (FixedArray v) = 4 + (fromIntegral (G.length v) * singleSize v undefined)
    where
      singleSize :: ByteSize b => f b -> b -> Int32
      singleSize _ x = byteSize x


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
  | StaleControllerEpochCode
  | OffsetMetadataTooLargeCode
  | OffsetsLoadInProgressCode
  | ConsumerCoordinatorNotAvailableCode
  | NotCoordinatorForConsumerCode
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
    11 -> StaleControllerEpochCode
    12 -> OffsetMetadataTooLargeCode
    14 -> OffsetsLoadInProgressCode
    15 -> ConsumerCoordinatorNotAvailableCode
    16 -> NotCoordinatorForConsumerCode
    _ -> Unknown
  fromEnum c = case c of
    NoError -> 0
    Unknown -> (-1)
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
    StaleControllerEpochCode -> 11
    OffsetMetadataTooLargeCode -> 12
    OffsetsLoadInProgressCode -> 14
    ConsumerCoordinatorNotAvailableCode -> 15
    NotCoordinatorForConsumerCode -> 16

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


newtype Partition = Partition { fromPartition :: Int32 }
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
  byteSize (Utf8 bs) = 2 + (fromIntegral $ BS.length bs)


data CompressionCodec = NoCompression
                      | GZip
                      | Snappy


newtype Attributes = Attributes { attributesCompression :: CompressionCodec }


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
  { messageCrc        :: !Int32
  , messageMagicByte  :: !Int8
  , messageAttributes :: !Attributes
  , messageKey        :: !ByteString
  , messageValue      :: !ByteString
  }

data MessageSet = MessageSet
  { messageSetOffset   :: !Int64
  , messageSetMessages :: !(V.Vector Message)
  }

data PartitionMessages = PartitionMessages
  { partitionMessagesPartition :: !PartitionId
  , partitionMessagesMessages  :: !MessageSet
  }



newtype GenerationId = GenerationId Int32
newtype RetentionTime = RetentionTime Int64



putL :: Binary a => Lens s t a b -> s -> Put
putL l = put . view l

data Request a v = Request
  { requestCorrelationId :: !CorrelationId
  , requestClientId      :: !Utf8
  , requestMessage       :: !(RequestMessage a v)
  } deriving (Generic)

deriving instance (Show (RequestMessage a v)) => Show (Request a v)

instance (KnownNat v, RequestApiKey a, ByteSize (RequestMessage a v), Binary (RequestMessage a v)) => Binary (Request a v) where
  get = do
    bytesToRead <- fromIntegral <$> getWord32be
    isolate bytesToRead $ do
      _ <- get :: Get ApiKey
      (Request <$> get <*> get <*> get)
  put p = do
    put $ byteSize p
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
  get = do
    bytesToRead <- fromIntegral <$> getWord32be
    isolate bytesToRead (Response <$> (trace "get corrId" get) <*> (trace "get request" get))
  put p = do
    put $ byteSize p
    put $ responseCorrelationId p
    put $ responseMessage p

deriving instance (Show (ResponseMessage a v)) => Show (Response a v)

instance (ByteSize (ResponseMessage a v)) => ByteSize (Response a v) where
  byteSize p = byteSize (responseCorrelationId p) +
               byteSize (responseMessage p)

instance (KnownNat v, ByteSize (RequestMessage a v)) => ByteSize (Request a v) where
  byteSize p = byteSize (undefined :: ApiKey) +
               byteSize (ApiVersion $ fromIntegral $ natVal p) +
               byteSize (requestCorrelationId p) +
               byteSize (requestClientId p) +
               byteSize (requestMessage p)
