{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
module Network.Kafka.Primitive.Offset where
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data instance RequestMessage Offset 0 = OffsetRequest
  { offsetRequestReplicaId       :: !NodeId
  , offsetRequestTopicPartitions :: !(V.Vector TopicPartition)
  } deriving (Show)

instance Binary (RequestMessage Offset 0) where
  get = OffsetRequest <$> get <*> (fromArray <$> get)
  put r = put (offsetRequestReplicaId r) *>
          put (Array $ offsetRequestTopicPartitions r)

instance ByteSize (RequestMessage Offset 0) where
  byteSize p = byteSize (offsetRequestReplicaId p) +
               byteSize (offsetRequestTopicPartitions p)


data TopicPartition = TopicPartition
  { topicPartitionTopic   :: !Utf8
  , topicPartitionOffsets :: !(V.Vector PartitionOffsetRequestInfo)
  } deriving (Show)

instance Binary TopicPartition where
  get = TopicPartition <$> get <*> (fromFixedArray <$> get)
  put t = put (topicPartitionTopic t) *>
          put (FixedArray $ topicPartitionOffsets t)

instance HasTopic TopicPartition Utf8 where
  topic = lens topicPartitionTopic (\s a -> s { topicPartitionTopic = a })
  {-# INLINEABLE topic #-}

instance ByteSize TopicPartition where
  byteSize p = byteSize (topicPartitionTopic p) +
               byteSize (FixedArray $ topicPartitionOffsets p)

instance HasOffsets TopicPartition (V.Vector PartitionOffsetRequestInfo) where
  offsets = lens topicPartitionOffsets (\s a -> s { topicPartitionOffsets = a })
  {-# INLINEABLE offsets #-}

data PartitionOffsetRequestInfo = PartitionOffsetRequestInfo
  { partitionOffsetRequestInfoPartition          :: !PartitionId
  , partitionOffsetRequestInfoTime               :: !Int64 -- ^ milliseconds
  , partitionOffsetRequestInfoMaxNumberOfOffsets :: !Int32
  } deriving (Show)

instance Binary PartitionOffsetRequestInfo where
  get = PartitionOffsetRequestInfo <$> get <*> get <*> get
  put p = put (partitionOffsetRequestInfoPartition p) *>
          put (partitionOffsetRequestInfoTime p) *>
          put (partitionOffsetRequestInfoMaxNumberOfOffsets p)

instance ByteSize PartitionOffsetRequestInfo where
  byteSize p = byteSize (partitionOffsetRequestInfoPartition p) +
               byteSize (partitionOffsetRequestInfoTime p) +
               byteSize (partitionOffsetRequestInfoMaxNumberOfOffsets p)

instance HasPartition PartitionOffsetRequestInfo PartitionId where
  partition = lens partitionOffsetRequestInfoPartition (\s a -> s { partitionOffsetRequestInfoPartition = a })
  {-# INLINEABLE partition #-}

instance HasTime PartitionOffsetRequestInfo Int64 where
  time = lens partitionOffsetRequestInfoTime (\s a -> s { partitionOffsetRequestInfoTime = a })
  {-# INLINEABLE time #-}

instance HasMaxNumberOfOffsets PartitionOffsetRequestInfo Int32 where
  maxNumberOfOffsets = lens partitionOffsetRequestInfoMaxNumberOfOffsets (\s a -> s { partitionOffsetRequestInfoMaxNumberOfOffsets = a })
  {-# INLINEABLE maxNumberOfOffsets #-}


data PartitionOffset = PartitionOffset
  { partitionOffsetPartition :: !Int32
  , partitionOffsetErrorCode :: !ErrorCode
  , partitionOffsetOffset    :: !Int64
  } deriving (Show)

instance Binary PartitionOffset where
  get = PartitionOffset <$> get <*> get <*> get
  put p = put (partitionOffsetPartition p) *>
          put (partitionOffsetErrorCode p) *>
          put (partitionOffsetOffset p)

instance ByteSize PartitionOffset where
  byteSize p = byteSize (partitionOffsetPartition p) +
               byteSize (partitionOffsetErrorCode p) +
               byteSize (partitionOffsetOffset p)

instance HasPartition PartitionOffset Int32 where
  partition = lens partitionOffsetPartition (\s a -> s { partitionOffsetPartition = a })
  {-# INLINEABLE partition #-}

instance HasErrorCode PartitionOffset ErrorCode where
  errorCode = lens partitionOffsetErrorCode (\s a -> s { partitionOffsetErrorCode = a })
  {-# INLINEABLE errorCode #-}

instance HasOffset PartitionOffset Int64 where
  offset = lens partitionOffsetOffset (\s a -> s { partitionOffsetOffset = a })
  {-# INLINEABLE offset #-}

data instance ResponseMessage Offset 0 = OffsetResponse
  { offsetResponseOffsets :: !(V.Vector PartitionOffsetResponseInfo)
  } deriving (Show)

instance Binary (ResponseMessage Offset 0) where
  get = OffsetResponse <$> (fromArray <$> get)
  put r = put (Array $ offsetResponseOffsets r)

instance ByteSize (ResponseMessage Offset 0) where
  byteSize = byteSize . FixedArray . offsetResponseOffsets

instance HasOffsets (ResponseMessage Offset 0) (V.Vector PartitionOffsetResponseInfo) where
  offsets = lens offsetResponseOffsets (\s a -> s { offsetResponseOffsets = a })
  {-# INLINEABLE offsets #-}


data PartitionOffsetResponseInfo = PartitionOffsetResponseInfo
  { partitionOffsetResponseInfoTopic   :: !Utf8
  , partitionOffsetResponseInfoOffsets :: !(V.Vector PartitionOffset)
  } deriving (Show)

instance Binary PartitionOffsetResponseInfo where
  get = PartitionOffsetResponseInfo <$> get <*> (fromFixedArray <$> get)
  put p = put (partitionOffsetResponseInfoTopic p) *>
          put (FixedArray $ partitionOffsetResponseInfoOffsets p)

instance ByteSize PartitionOffsetResponseInfo where
  byteSize p = byteSize (partitionOffsetResponseInfoTopic p) +
               byteSize (partitionOffsetResponseInfoOffsets p)

instance HasTopic PartitionOffsetResponseInfo Utf8 where
  topic = lens partitionOffsetResponseInfoTopic (\s a -> s { partitionOffsetResponseInfoTopic = a })
  {-# INLINEABLE topic #-}

instance HasOffsets PartitionOffsetResponseInfo (V.Vector PartitionOffset) where
  offsets = lens partitionOffsetResponseInfoOffsets (\s a -> s { partitionOffsetResponseInfoOffsets = a })
  {-# INLINEABLE offsets #-}
