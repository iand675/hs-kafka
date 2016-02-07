{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}
module Network.Kafka.Primitive.Metadata where
import           Network.Kafka.Exports
import           Network.Kafka.Types
import qualified Data.Vector as V

data instance RequestMessage Metadata 0 = MetadataRequestV0
  { metadataRequestV0Topics :: !(V.Vector Utf8)
  } deriving (Eq, Show, Generic)

instance Binary (RequestMessage Metadata 0) where
  get = MetadataRequestV0 <$> (fromArray <$> get)
  put m = put (Array $ view topics m)

instance ByteSize (RequestMessage Metadata 0) where
  byteSize = byteSize . metadataRequestV0Topics

instance HasTopics (RequestMessage Metadata 0) (V.Vector Utf8) where
  topics = lens metadataRequestV0Topics (\s a -> s { metadataRequestV0Topics = a })
  {-# INLINEABLE topics #-}


data instance ResponseMessage Metadata 0 = MetadataResponseV0
  { metadataResponseV0Brokers :: !(V.Vector Broker)
  , metadataResponseV0Topics  :: !(V.Vector TopicMetadata)
  } deriving (Eq, Show, Generic)

instance Binary (ResponseMessage Metadata 0) where
  get = MetadataResponseV0 <$> (fromArray <$> get) <*> (fromArray <$> get)
  put m = put (Array $ view brokers m) *> put (Array $ view topics m)

instance ByteSize (ResponseMessage Metadata 0) where
  byteSize m = -- byteSize (metadataResponseCorrelationId m) +
               byteSize (metadataResponseV0Brokers m) +
               byteSize (metadataResponseV0Topics m)

instance HasBrokers (ResponseMessage Metadata 0) (V.Vector Broker) where
  brokers = lens metadataResponseV0Brokers (\s a -> s { metadataResponseV0Brokers = a })
  {-# INLINEABLE brokers #-}

instance HasTopics (ResponseMessage Metadata 0) (V.Vector TopicMetadata) where
  topics = lens metadataResponseV0Topics (\s a -> s { metadataResponseV0Topics = a })
  {-# INLINEABLE topics #-}


data Broker = Broker
  { brokerNodeId :: !NodeId
  , brokerHost   :: !Utf8
  , brokerPort   :: !Int32
  } deriving (Show, Eq, Generic)

instance Binary Broker where
  get = Broker <$> get <*> get <*> get
  put b = putL nodeId b *> putL host b *> putL port b

instance ByteSize Broker where
  byteSize b = byteSize (brokerNodeId b) +
               byteSize (brokerHost b) +
               byteSize (brokerPort b)

instance HasNodeId Broker NodeId where
  nodeId = lens brokerNodeId (\s a -> s { brokerNodeId = a })
  {-# INLINEABLE nodeId #-}

instance HasHost Broker Utf8 where
  host = lens brokerHost (\s a -> s { brokerHost = a })
  {-# INLINEABLE host #-}

instance HasPort Broker Int32 where
  port = lens brokerPort (\s a -> s { brokerPort = a })
  {-# INLINEABLE port #-}

data TopicMetadata = TopicMetadata
  { topicMetadataErrorCode         :: !ErrorCode
  , topicMetadataTopic             :: !Utf8
  , topicMetadataPartitionMetadata :: !(V.Vector PartitionMetadata)
  } deriving (Eq, Show, Generic)

instance Binary TopicMetadata where
  get = TopicMetadata <$> get <*> get <*> (fromArray <$> get)
  put t = putL errorCode t *>
          putL topic t *>
          put (Array $ topicMetadataPartitionMetadata t)

instance ByteSize TopicMetadata where
  byteSize t = byteSize (topicMetadataErrorCode t) +
               byteSize (topicMetadataTopic t) +
               byteSize (topicMetadataPartitionMetadata t)


instance HasErrorCode TopicMetadata ErrorCode where
  errorCode = lens topicMetadataErrorCode (\s a -> s { topicMetadataErrorCode = a })
  {-# INLINEABLE errorCode #-}

instance HasTopic TopicMetadata Utf8 where
  topic = lens topicMetadataTopic (\s a -> s { topicMetadataTopic = a })
  {-# INLINEABLE topic #-}

instance HasPartitionMetadata TopicMetadata (V.Vector PartitionMetadata) where
  partitionMetadata = lens topicMetadataPartitionMetadata (\s a -> s { topicMetadataPartitionMetadata = a })
  {-# INLINEABLE partitionMetadata #-}


data PartitionMetadata = PartitionMetadata
  { partitionMetadataErrorCode    :: !ErrorCode
  , partitionMetadataPartition    :: !PartitionId
  , partitionMetadataLeader       :: !NodeId
  , partitionMetadataReplicas     :: !(V.Vector NodeId)
  , partitionMetadataIsReplicated :: !(V.Vector NodeId)
  } deriving (Eq, Show, Generic)

instance Binary PartitionMetadata where
  get = PartitionMetadata <$> get <*> get <*> get <*> (fromFixedArray <$> get) <*> (fromFixedArray <$> get)
  put p = putL errorCode p *>
          putL partition p *>
          putL leader p *>
          put (FixedArray $ partitionMetadataReplicas p) *>
          put (FixedArray $ partitionMetadataIsReplicated p)

instance ByteSize PartitionMetadata where
  byteSize p = byteSize (partitionMetadataErrorCode p) +
               byteSize (partitionMetadataPartition p) +
               byteSize (partitionMetadataLeader p) +
               byteSize (FixedArray $ partitionMetadataReplicas p) +
               byteSize (FixedArray $ partitionMetadataIsReplicated p)

instance HasErrorCode PartitionMetadata ErrorCode where
  errorCode = lens partitionMetadataErrorCode (\s a -> s { partitionMetadataErrorCode = a })
  {-# INLINEABLE errorCode #-}

instance HasPartition PartitionMetadata PartitionId where
  partition = lens partitionMetadataPartition (\s a -> s { partitionMetadataPartition = a })
  {-# INLINEABLE partition #-}

instance HasLeader PartitionMetadata NodeId where
  leader = lens partitionMetadataLeader (\s a -> s { partitionMetadataLeader = a })
  {-# INLINEABLE leader #-}

instance HasReplicas PartitionMetadata (V.Vector NodeId) where
  replicas = lens partitionMetadataReplicas (\s a -> s { partitionMetadataReplicas = a })
  {-# INLINEABLE replicas #-}

instance HasIsReplicated PartitionMetadata (V.Vector NodeId) where
  isReplicated = lens partitionMetadataIsReplicated (\s a -> s { partitionMetadataIsReplicated = a })
  {-# INLINEABLE isReplicated #-}
