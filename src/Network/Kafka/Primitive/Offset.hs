{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE TypeFamilies           #-}
module Network.Kafka.Primitive.Offset where
import           Control.Lens
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data PartitionOffsetRequestInfo = PartitionOffsetRequestInfo
  { partitionOffsetRequestInfoPartition          :: !PartitionId
  , partitionOffsetRequestInfoTime               :: !Int64 -- ^ milliseconds
  , partitionOffsetRequestInfoMaxNumberOfOffsets :: !Int32
  } deriving (Eq, Show, Generic)


instance Binary PartitionOffsetRequestInfo where
  get = PartitionOffsetRequestInfo <$> get <*> get <*> get
  put p = put (partitionOffsetRequestInfoPartition p) *>
          put (partitionOffsetRequestInfoTime p) *>
          put (partitionOffsetRequestInfoMaxNumberOfOffsets p)

instance ByteSize PartitionOffsetRequestInfo where
  byteSize p = byteSize (partitionOffsetRequestInfoPartition p) +
               byteSize (partitionOffsetRequestInfoTime p) +
               byteSize (partitionOffsetRequestInfoMaxNumberOfOffsets p)


data TopicPartition = TopicPartition
  { topicPartitionTopic   :: !Utf8
  , topicPartitionOffsets :: !(V.Vector PartitionOffsetRequestInfo)
  } deriving (Eq, Show, Generic)


instance Binary TopicPartition where
  get = TopicPartition <$> get <*> (fromFixedArray <$> get)
  put t = put (topicPartitionTopic t) *>
          put (FixedArray $ topicPartitionOffsets t)

instance ByteSize TopicPartition where
  byteSize p = byteSize (topicPartitionTopic p) +
               byteSize (FixedArray $ topicPartitionOffsets p)


data OffsetRequestV0 = OffsetRequestV0
  { offsetRequestV0ReplicaId       :: !NodeId
  , offsetRequestV0TopicPartitions :: !(V.Vector TopicPartition)
  } deriving (Eq, Show, Generic)

instance RequestApiKey OffsetRequestV0 where
  apiKey = theApiKey 2

instance RequestApiVersion OffsetRequestV0 where
  apiVersion = const 0


instance Binary OffsetRequestV0 where
  get = OffsetRequestV0 <$> get <*> (fromArray <$> get)
  put r = put (offsetRequestV0ReplicaId r) *>
          put (Array $ offsetRequestV0TopicPartitions r)

instance ByteSize OffsetRequestV0 where
  byteSize p = byteSize (offsetRequestV0ReplicaId p) +
               byteSize (offsetRequestV0TopicPartitions p)


data PartitionOffset = PartitionOffset
  { partitionOffsetPartition :: !Int32
  , partitionOffsetErrorCode :: !ErrorCode
  , partitionOffsetOffset    :: !Int64
  } deriving (Eq, Show, Generic)


instance Binary PartitionOffset where
  get = PartitionOffset <$> get <*> get <*> get
  put p = put (partitionOffsetPartition p) *>
          put (partitionOffsetErrorCode p) *>
          put (partitionOffsetOffset p)

instance ByteSize PartitionOffset where
  byteSize p = byteSize (partitionOffsetPartition p) +
               byteSize (partitionOffsetErrorCode p) +
               byteSize (partitionOffsetOffset p)

data PartitionOffsetResponseInfo = PartitionOffsetResponseInfo
  { partitionOffsetResponseInfoTopic   :: !Utf8
  , partitionOffsetResponseInfoOffsets :: !(V.Vector PartitionOffset)
  } deriving (Eq, Show, Generic)


instance Binary PartitionOffsetResponseInfo where
  get = PartitionOffsetResponseInfo <$> get <*> (fromFixedArray <$> get)
  put p = put (partitionOffsetResponseInfoTopic p) *>
          put (FixedArray $ partitionOffsetResponseInfoOffsets p)

instance ByteSize PartitionOffsetResponseInfo where
  byteSize p = byteSize (partitionOffsetResponseInfoTopic p) +
               byteSize (partitionOffsetResponseInfoOffsets p)


newtype OffsetResponseV0 = OffsetResponseV0
  { offsetResponseV0Offsets :: V.Vector PartitionOffsetResponseInfo
  } deriving (Eq, Show, Generic)


instance Binary OffsetResponseV0 where
  get = OffsetResponseV0 <$> (fromArray <$> get)
  put r = put (Array $ offsetResponseV0Offsets r)

instance ByteSize OffsetResponseV0 where
  byteSize = byteSize . FixedArray . offsetResponseV0Offsets


