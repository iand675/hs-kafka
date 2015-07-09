{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE StandaloneDeriving #-}
module Network.Kafka where

import Control.Monad
import Data.Int
import Data.IORef
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.Monoid
import qualified Data.Vector as V
import qualified Data.Vector.Generic as G
import GHC.Generics
import Network.Simple.TCP hiding (send)
import qualified Network.Socket.ByteString.Lazy as Lazy

import Prelude hiding (putStr)

import Debug.Trace

newtype Array v a = Array { fromArray :: v a }
  deriving (Show)

instance (Binary a, G.Vector v a) => Binary (Array v a) where
  get = do
    len <- fromIntegral <$> getWord32be
    Array <$> G.replicateM len get
  put (Array v) = do
    putWord32be $ fromIntegral $ G.length v
    G.mapM_ put v

newtype FixedArray v a = FixedArray { fromFixedArray :: v a }

instance (Binary a, G.Vector v a) => Binary (FixedArray v a) where
  get = (FixedArray . fromArray) <$> get
  put = put . Array . fromFixedArray

instance (ByteSize a, G.Vector v a) => ByteSize (FixedArray v a) where
  byteSize v = 4 + (fromIntegral (G.length $ fromFixedArray v) * byteSize (undefined :: a))

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
  deriving (Show, Eq)

instance Enum ErrorCode where
  toEnum c = case c of
    0 -> NoError
    (-1) -> Unknown
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

-- | Note that for requests and responses, only NON-COMMON FIELDS should be counted in the instance
class ByteSize a where
  byteSize :: a -> Int32

instance ByteSize Int16 where
  byteSize = const 2

instance ByteSize Int32 where
  byteSize = const 4

instance ByteSize Int64 where
  byteSize = const 8

instance ByteSize ErrorCode where
  byteSize = const 2

instance ByteSize a => ByteSize (V.Vector a) where
  byteSize v = 4 + (G.sum $ G.map byteSize v)

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

newtype Utf8 = Utf8 { fromUtf8 :: BS.ByteString }
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

class (ByteSize (RequestMessage p), ByteSize (ResponseMessage p), Binary (RequestMessage p), Binary (ResponseMessage p)) => RequestApiKey p where
  apiKey :: Request p -> ApiKey

data CompressionCodec = NoCompression
                      | GZip
                      | Snappy

newtype Attributes = Attributes { attributesCompression :: CompressionCodec }

data family RequestMessage p
data family ResponseMessage p

{-
-- TODO grok compression
data Message a = Message
  { messageKey        :: !BL.ByteString
  , messageValue      :: !BL.ByteString
  }

data ProduceMessage = ProduceMessage
  { produceMessagePartition :: !Int32
  , produceMessageSet       :: !MessageSet
  }

data instance Request Produce = ProduceRequest
  { produceRequestRequiredAcks :: !Int16
  , produceRequestTimeout      :: !Int32 -- ^ milliseconds
  , produceRequestMessages     :: !(V.Vector ProduceMessage)
  } deriving (Show)
-}

data Produce
data Fetch
data Offset
data Metadata
data OffsetCommit
data OffsetFetch
data ConsumerMetadata

data Broker = Broker
  { brokerNodeId :: !NodeId
  , brokerHost   :: !Utf8
  , brokerPort   :: !Int32
  } deriving (Show)

instance ByteSize Broker where
  byteSize b = byteSize (brokerNodeId b) +
               byteSize (brokerHost b) +
               byteSize (brokerPort b)

instance Binary Broker where
  get = Broker <$> get <*> get <*> get
  put b = put (brokerNodeId b) *> put (brokerHost b) *> put (brokerPort b)

data TopicMetadata = TopicMetadata
  { topicMetadataErrorCode         :: !ErrorCode
  , topicMetadataTopicName         :: !Utf8
  , topicMetadataPartitionMetadata :: !(V.Vector PartitionMetadata)
  } deriving (Show)

instance ByteSize TopicMetadata where
  byteSize t = byteSize (topicMetadataErrorCode t) +
               byteSize (topicMetadataTopicName t) +
               byteSize (topicMetadataPartitionMetadata t)

instance Binary TopicMetadata where
  get = TopicMetadata <$> get <*> get <*> (fromArray <$> get)
  put t = put (topicMetadataErrorCode t) *>
          put (topicMetadataTopicName t) *>
          put (Array $ topicMetadataPartitionMetadata t)

data PartitionMetadata = PartitionMetadata
  { partitionMetadataErrorCode    :: !ErrorCode
  , partitionMetadataPartitionId  :: !PartitionId
  , partitionMetadataLeader       :: !NodeId
  , partitionMetadataReplicas     :: !(V.Vector NodeId)
  , partitionMetadataIsReplicated :: !(V.Vector NodeId)
  } deriving (Show)

instance ByteSize PartitionMetadata where
  byteSize p = byteSize (partitionMetadataErrorCode p) +
               byteSize (partitionMetadataPartitionId p) +
               byteSize (partitionMetadataLeader p) +
               byteSize (FixedArray $ partitionMetadataReplicas p) +
               byteSize (FixedArray $ partitionMetadataIsReplicated p)

instance Binary PartitionMetadata where
  get = PartitionMetadata <$> get <*> get <*> get <*> (fromFixedArray <$> get) <*> (fromFixedArray <$> get)
  put p = put (partitionMetadataErrorCode p) *>
          put (partitionMetadataPartitionId p) *>
          put (partitionMetadataLeader p) *>
          put (FixedArray $ partitionMetadataReplicas p) *>
          put (FixedArray $ partitionMetadataIsReplicated p)

data instance RequestMessage Metadata = MetadataRequest
  { metadataRequestTopicNames    :: !(V.Vector Utf8)
  } deriving (Show, Generic)

data instance ResponseMessage Metadata = MetadataResponse
  { metadataResponseBrokers       :: !(V.Vector Broker)
  , metadataResponseTopics        :: !(V.Vector TopicMetadata)
  } deriving (Show, Generic)


data instance RequestMessage ConsumerMetadata = ConsumerMetadataRequest
  { consumerMetadataRequestConsumerGroup :: !Utf8
  } deriving (Show, Generic)

data instance ResponseMessage ConsumerMetadata = ConsumerMetadataResponse
  { consumerMetadataResponseErrorCode       :: !ErrorCode
  , consumerMetadataResponseCoordinatorId   :: !CoordinatorId
  , consumerMetadataResponseCoordinatorHost :: !Utf8
  , consumerMetadataResponseCoordinatorPort :: !Int32
  } deriving (Show, Generic)

theApiKey :: Int16 -> a -> ApiKey
theApiKey x = const $ ApiKey x

{-
instance RequestApiKey Produce where
  apiKey = theApiKey 0

instance RequestApiKey Fetch where
  apiKey = theApiKey 1
-}

data PartitionOffsetRequestInfo = PartitionOffsetRequestInfo
  { partitionOffsetRequestInfoPartition          :: !Int32
  , partitionOffsetRequestInfoTime               :: !Int64 -- ^ milliseconds
  , partitionOffsetRequestInfoMaxNumberOfOffsets :: !Int32
  } deriving (Show)

instance ByteSize PartitionOffsetRequestInfo where
  byteSize p = byteSize (partitionOffsetRequestInfoPartition p) +
               byteSize (partitionOffsetRequestInfoTime p) +
               byteSize (partitionOffsetRequestInfoMaxNumberOfOffsets p)

instance Binary PartitionOffsetRequestInfo where
  get = PartitionOffsetRequestInfo <$> get <*> get <*> get
  put p = put (partitionOffsetRequestInfoPartition p) *>
          put (partitionOffsetRequestInfoTime p) *>
          put (partitionOffsetRequestInfoMaxNumberOfOffsets p)

data TopicPartition = TopicPartition
  { topicPartitionTopicName :: !Utf8
  , topicPartitionOffsets   :: !(V.Vector PartitionOffsetRequestInfo)
  } deriving (Show)

instance ByteSize TopicPartition where
  byteSize p = byteSize (topicPartitionTopicName p) +
               byteSize (FixedArray $ topicPartitionOffsets p)

instance Binary TopicPartition where
  get = TopicPartition <$> get <*> (fromFixedArray <$> get)
  put t = put (topicPartitionTopicName t) *>
          put (FixedArray $ topicPartitionOffsets t)

data instance RequestMessage Offset = OffsetRequest
  { offsetRequestReplicaId       :: !NodeId
  , offsetRequestTopicPartitions :: !(V.Vector TopicPartition)
  } deriving (Show)

data PartitionOffset = PartitionOffset
  { partitionOffsetPartition :: !Int32
  , partitionOffsetErrorCode :: !ErrorCode
  , partitionOffsetOffset    :: !Int64
  } deriving (Show)

instance ByteSize PartitionOffset where
  byteSize p = byteSize (partitionOffsetPartition p) +
               byteSize (partitionOffsetErrorCode p) +
               byteSize (partitionOffsetOffset p)

instance Binary PartitionOffset where
  get = PartitionOffset <$> get <*> get <*> get
  put p = put (partitionOffsetPartition p) *>
          put (partitionOffsetErrorCode p) *>
          put (partitionOffsetOffset p)

data PartitionOffsetResponseInfo = PartitionOffsetResponseInfo
  { partitionOffsetResponseInfoTopicName        :: !Utf8
  , partitionOffsetResponseInfoPartitionOffsets :: !(V.Vector PartitionOffset)
  } deriving (Show)

instance ByteSize PartitionOffsetResponseInfo where
  byteSize p = byteSize (partitionOffsetResponseInfoTopicName p) +
               byteSize (partitionOffsetResponseInfoPartitionOffsets p)

instance Binary PartitionOffsetResponseInfo where
  get = PartitionOffsetResponseInfo <$> get <*> (fromFixedArray <$> get)
  put p = put (partitionOffsetResponseInfoTopicName p) *>
          put (FixedArray $ partitionOffsetResponseInfoPartitionOffsets p)

data instance ResponseMessage Offset = OffsetResponse
  { offsetResponseOffsets :: !(V.Vector PartitionOffsetResponseInfo)
  } deriving (Show)

instance ByteSize (RequestMessage Offset) where
  byteSize p = byteSize (offsetRequestReplicaId p) +
               byteSize (offsetRequestTopicPartitions p)

instance Binary (RequestMessage Offset) where
  get = OffsetRequest <$> get <*> (fromArray <$> get)
  put r = put (offsetRequestReplicaId r) *>
          put (Array $ offsetRequestTopicPartitions r)

instance ByteSize (ResponseMessage Offset) where
  byteSize = byteSize . FixedArray . offsetResponseOffsets

instance Binary (ResponseMessage Offset) where
  get = OffsetResponse <$> trace "get offset response array" (fromArray <$> get)
  put r = put (Array $ offsetResponseOffsets r)

instance RequestApiKey Offset where
  apiKey = theApiKey 2

instance ByteSize (RequestMessage Metadata) where
  byteSize = byteSize . metadataRequestTopicNames

instance Binary (RequestMessage Metadata) where
  get = MetadataRequest <$> (fromArray <$> get)
  put m = put (Array $ metadataRequestTopicNames m)

instance ByteSize (ResponseMessage Metadata) where
  byteSize m = -- byteSize (metadataResponseCorrelationId m) +
               byteSize (metadataResponseBrokers m) +
               byteSize (metadataResponseTopics m)

instance Binary (ResponseMessage Metadata) where
  get = MetadataResponse <$> (fromArray <$> get) <*> (fromArray <$> get)
  put m = put (Array $ metadataResponseBrokers m) *> put (Array $ metadataResponseTopics m)

instance RequestApiKey Metadata where
  apiKey = theApiKey 3

{-
instance RequestApiKey OffsetCommit where
  apiKey = theApiKey 8

instance RequestApiKey OffsetFetch where
  apiKey = theApiKey 9
-}

instance ByteSize (RequestMessage ConsumerMetadata) where
  byteSize = byteSize . consumerMetadataRequestConsumerGroup

instance Binary (RequestMessage ConsumerMetadata) where
  get = ConsumerMetadataRequest <$> get
  put = put . consumerMetadataRequestConsumerGroup

instance ByteSize (ResponseMessage ConsumerMetadata) where
  byteSize r = byteSize (consumerMetadataResponseErrorCode r) +
               byteSize (consumerMetadataResponseCoordinatorId r) +
               byteSize (consumerMetadataResponseCoordinatorHost r) +
               byteSize (consumerMetadataResponseCoordinatorPort r)

instance Binary (ResponseMessage ConsumerMetadata) where
  get = ConsumerMetadataResponse <$> get <*> get <*> get <*> get 
  put r = do
    put $ consumerMetadataResponseErrorCode r
    put $ consumerMetadataResponseCoordinatorId r
    put $ consumerMetadataResponseCoordinatorHost r
    put $ consumerMetadataResponseCoordinatorPort r

instance RequestApiKey ConsumerMetadata where
  apiKey = theApiKey 10

check :: Binary a => a -> (BL.ByteString, Int64, a)
check x = let bs = runPut $ put x in (BL.drop 4 bs, BL.length bs - 4, runGet get bs)

data KafkaClient = KafkaClient { kafkaSocket    :: Socket
                               , kafkaLeftovers :: IORef BL.ByteString
                               }

withKafkaClient :: HostName -> ServiceName -> (KafkaClient -> IO a) -> IO a
withKafkaClient h p f = connect h p $ \(s, _) -> do
  ref <- newIORef =<< Lazy.getContents s
  f $ KafkaClient s ref

send :: (RequestApiKey p, Binary (Request p), Binary (Response p)) => KafkaClient -> Request p -> IO (Response p)
send c req = do
  void $ Lazy.send (kafkaSocket c) $ runPut $ put req
  left <- readIORef $ kafkaLeftovers c
  case runGetOrFail get left of
    Left (_, _, err) -> error err
    Right (rest, _, x) -> writeIORef (kafkaLeftovers c) rest >> return x
  {-
  go $ runGetIncremental get
  where
    go (Partial cb) = do
      old <- atomicModifyIORef' (kafkaLeftovers c) $ \old -> (Nothing, old)
      case old of
        Nothing -> do
          chunk <- recv (kafkaSocket c) 4096
          print chunk
          go $ cb chunk
        c -> do
          print c
          go $ cb c
    go (Done left off resp) = do
      modifyIORef' (kafkaLeftovers c) (\leftOld -> leftOld <> if BS.null left then Nothing else Just left)
      putStrLn "done"
      return resp
    go (Fail bs off err) = do
      putStrLn "error"
      error err
  -}

data Request a = Request
  { requestApiVersion    :: !ApiVersion
  , requestCorrelationId :: !CorrelationId
  , requestClientId      :: !Utf8
  , requestMessage       :: !(RequestMessage a)
  } deriving (Generic)

req :: CorrelationId -> Utf8 -> RequestMessage a -> Request a
req = Request (ApiVersion 0)

deriving instance (Show (RequestMessage a)) => Show (Request a)

instance (ByteSize (RequestMessage a)) => ByteSize (Request a) where
  byteSize p = byteSize (undefined :: ApiKey) +
               byteSize (requestApiVersion p) +
               byteSize (requestCorrelationId p) +
               byteSize (requestClientId p) +
               byteSize (requestMessage p)

instance (RequestApiKey a) => Binary (Request a) where
  get = do
    bytesToRead <- fromIntegral <$> getWord32be
    isolate bytesToRead $ do
      _ <- get :: Get ApiKey
      (Request <$> get <*> get <*> get <*> get)
  put p = do
    put $ byteSize p
    put $ apiKey p
    put $ requestApiVersion p
    put $ requestCorrelationId p
    put $ requestClientId p
    put $ requestMessage p

data Response a = Response
  { responseCorrelationId :: !CorrelationId
  , responseMessage       :: !(ResponseMessage a)
  } deriving (Generic)

deriving instance (Show (ResponseMessage a)) => Show (Response a)

instance (ByteSize (ResponseMessage a)) => ByteSize (Response a) where
  byteSize p = byteSize (responseCorrelationId p) +
               byteSize (responseMessage p)

instance (RequestApiKey a) => Binary (Response a) where
  get = do
    bytesToRead <- fromIntegral <$> getWord32be
    isolate bytesToRead (Response <$> (trace "get corrId" get) <*> (trace "get request" get))
  put p = do
    put $ byteSize p
    put $ responseCorrelationId p
    put $ responseMessage p
