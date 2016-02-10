{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE TypeFamilies           #-}
module Network.Kafka.Primitive.Metadata where
import           Control.Lens
import           Network.Kafka.Exports
import           Network.Kafka.Types
import qualified Data.Vector as V

newtype MetadataRequestV0 = MetadataRequestV0
  { metadataRequestV0Topics :: V.Vector Utf8
  } deriving (Eq, Show, Generic)

instance RequestApiKey MetadataRequestV0 where
  apiKey = theApiKey 3

instance RequestApiVersion MetadataRequestV0 where
  apiVersion = const 0


instance Binary MetadataRequestV0 where
  get = MetadataRequestV0 <$> (fromArray <$> get)
  put = put . Array . metadataRequestV0Topics

instance ByteSize MetadataRequestV0 where
  byteSize = byteSize . metadataRequestV0Topics

data Broker = Broker
  { brokerNodeId :: !NodeId
  , brokerHost   :: !Utf8
  , brokerPort   :: !Int32
  } deriving (Show, Eq, Generic)


instance Binary Broker where
  get = Broker <$> get <*> get <*> get
  put b = do
    put $ brokerNodeId b
    put $ brokerHost b
    put $ brokerPort b

instance ByteSize Broker where
  byteSize b = byteSize (brokerNodeId b) +
               byteSize (brokerHost b) +
               byteSize (brokerPort b)

data PartitionMetadata = PartitionMetadata
  { partitionMetadataErrorCode    :: !ErrorCode
  , partitionMetadataPartition    :: !PartitionId
  , partitionMetadataLeader       :: !NodeId
  , partitionMetadataReplicas     :: !(V.Vector NodeId)
  , partitionMetadataIsReplicated :: !(V.Vector NodeId)
  } deriving (Eq, Show, Generic)


instance Binary PartitionMetadata where
  get = PartitionMetadata <$> get <*> get <*> get <*> (fromFixedArray <$> get) <*> (fromFixedArray <$> get)
  put p = do
    put $ partitionMetadataErrorCode p
    put $ partitionMetadataPartition p
    put $ partitionMetadataLeader p
    put $ FixedArray $ partitionMetadataReplicas p
    put $ FixedArray $ partitionMetadataIsReplicated p

instance ByteSize PartitionMetadata where
  byteSize p = byteSize (partitionMetadataErrorCode p) +
               byteSize (partitionMetadataPartition p) +
               byteSize (partitionMetadataLeader p) +
               byteSize (FixedArray $ partitionMetadataReplicas p) +
               byteSize (FixedArray $ partitionMetadataIsReplicated p)

data TopicMetadata = TopicMetadata
  { topicMetadataErrorCode         :: !ErrorCode
  , topicMetadataTopic             :: !Utf8
  , topicMetadataPartitionMetadata :: !(V.Vector PartitionMetadata)
  } deriving (Eq, Show, Generic)


instance Binary TopicMetadata where
  get = TopicMetadata <$> get <*> get <*> (fromArray <$> get)
  put t = do
    put $ topicMetadataErrorCode t
    put $ topicMetadataTopic t 
    put $ Array $ topicMetadataPartitionMetadata t

instance ByteSize TopicMetadata where
  byteSize t = byteSize (topicMetadataErrorCode t) +
               byteSize (topicMetadataTopic t) +
               byteSize (topicMetadataPartitionMetadata t)

data MetadataResponseV0 = MetadataResponseV0
  { metadataResponseV0Brokers :: !(V.Vector Broker)
  , metadataResponseV0Topics  :: !(V.Vector TopicMetadata)
  } deriving (Eq, Show, Generic)


instance Binary MetadataResponseV0 where
  get = MetadataResponseV0 <$> (fromArray <$> get) <*> (fromArray <$> get)
  put m = do
    put $ Array $ metadataResponseV0Brokers m
    put $ Array $ metadataResponseV0Topics m

instance ByteSize MetadataResponseV0 where
  byteSize m = -- byteSize (metadataResponseCorrelationId m) +
               byteSize (metadataResponseV0Brokers m) +
               byteSize (metadataResponseV0Topics m)


