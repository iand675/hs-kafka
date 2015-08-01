{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}
module Network.Kafka.Primitive.OffsetFetch where
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data instance RequestMessage OffsetFetch 0 = OffsetFetchRequestV0
  { offsetFetchRequestV0ConsumerGroup :: !Utf8
  , offsetFetchRequestV0Topics        :: (V.Vector TopicOffset)
  }

instance Binary (RequestMessage OffsetFetch 0) where
  get = OffsetFetchRequestV0 <$> get <*> (fromArray <$> get)
  put o = do
    putL consumerGroup o
    putL (topics . to Array) o

instance ByteSize (RequestMessage OffsetFetch 0) where
  byteSize r = byteSizeL consumerGroup r + byteSizeL topics r

instance HasConsumerGroup (RequestMessage OffsetFetch 0) Utf8 where
  consumerGroup = lens offsetFetchRequestV0ConsumerGroup (\s a -> s { offsetFetchRequestV0ConsumerGroup = a })
  {-# INLINEABLE consumerGroup #-}

instance HasTopics (RequestMessage OffsetFetch 0) (V.Vector TopicOffset) where
  topics = lens offsetFetchRequestV0Topics (\s a -> s { offsetFetchRequestV0Topics = a })
  {-# INLINEABLE topics #-}



data TopicOffset = TopicOffset
  { topicOffsetTopic      :: !Utf8
  , topicOffsetPartitions :: !(V.Vector PartitionId)
  }

instance Binary TopicOffset where
  get = TopicOffset <$> get <*> (fromFixedArray <$> get)
  put t = do
    putL topic t
    putL (partitions . to FixedArray) t

instance ByteSize TopicOffset where
  byteSize t = byteSizeL topic t + byteSizeL (partitions . to FixedArray) t

instance HasTopic TopicOffset Utf8 where
  topic = lens topicOffsetTopic (\s a -> s { topicOffsetTopic = a })
  {-# INLINEABLE topic #-}

instance HasPartitions TopicOffset (V.Vector PartitionId) where
  partitions = lens topicOffsetPartitions (\s a -> s { topicOffsetPartitions = a })
  {-# INLINEABLE partitions #-}



data instance ResponseMessage OffsetFetch 0 = OffsetFetchResponseV0
  { offsetFetchResponseV0Topics :: !(V.Vector TopicOffsetResponse)
  }

instance Binary (ResponseMessage OffsetFetch 0) where
  get = (OffsetFetchResponseV0 . fromArray) <$> get
  put r = putL (topics . to Array) r

instance ByteSize (ResponseMessage OffsetFetch 0) where
  byteSize r = byteSizeL topics r

instance HasTopics (ResponseMessage OffsetFetch 0) (V.Vector TopicOffsetResponse) where
  topics = lens offsetFetchResponseV0Topics (\s a -> s { offsetFetchResponseV0Topics = a })
  {-# INLINEABLE topics #-}



data TopicOffsetResponse = TopicOffsetResponse
  { topicOffsetResponseTopic      :: !Utf8 
  , topicOffsetResponsePartitions :: !(V.Vector PartitionOffset)
  }

instance Binary TopicOffsetResponse where
  get = TopicOffsetResponse <$> get <*> (fromArray <$> get)
  put r = do
    putL topic r
    putL (partitions . to Array) r

instance ByteSize TopicOffsetResponse where
  byteSize r = byteSizeL topic r +
               byteSizeL partitions r

instance HasTopic TopicOffsetResponse Utf8 where
  topic = lens topicOffsetResponseTopic (\s a -> s { topicOffsetResponseTopic = a })
  {-# INLINEABLE topic #-}

instance HasPartitions TopicOffsetResponse (V.Vector PartitionOffset) where
  partitions = lens topicOffsetResponsePartitions (\s a -> s { topicOffsetResponsePartitions = a })
  {-# INLINEABLE partitions #-}



data PartitionOffset = PartitionOffset
  { partitionOffsetPartition :: !PartitionId
  , partitionOffsetOffset    :: !Int64
  , partitionOffsetMetadata  :: !Utf8
  , partitionOffsetErrorCode :: !ErrorCode
  }

instance Binary PartitionOffset where
  get = PartitionOffset <$> get <*> get <*> get <*> get
  put p = do
    putL partition p
    putL offset p
    putL metadata p
    putL errorCode p

instance ByteSize PartitionOffset where
  byteSize p = byteSizeL partition p +
               byteSizeL offset p +
               byteSizeL metadata p +
               byteSizeL errorCode p

instance HasPartition PartitionOffset PartitionId where
  partition = lens partitionOffsetPartition (\s a -> s { partitionOffsetPartition = a })
  {-# INLINEABLE partition #-}

instance HasOffset PartitionOffset Int64 where
  offset = lens partitionOffsetOffset (\s a -> s { partitionOffsetOffset = a })
  {-# INLINEABLE offset #-}

instance HasMetadata PartitionOffset Utf8 where
  metadata = lens partitionOffsetMetadata (\s a -> s { partitionOffsetMetadata = a })
  {-# INLINEABLE metadata #-}

instance HasErrorCode PartitionOffset ErrorCode where
  errorCode = lens partitionOffsetErrorCode (\s a -> s { partitionOffsetErrorCode = a })
  {-# INLINEABLE errorCode #-}



data instance RequestMessage OffsetFetch 1 = OffsetFetchRequestV1 !(RequestMessage OffsetFetch 0)

instance Binary (RequestMessage OffsetFetch 1) where
  get = OffsetFetchRequestV1 <$> get
  put (OffsetFetchRequestV1 r) = put r

instance ByteSize (RequestMessage OffsetFetch 1) where
  byteSize (OffsetFetchRequestV1 r) = byteSize r



data instance ResponseMessage OffsetFetch 1 = OffsetFetchResponseV1 !(ResponseMessage OffsetFetch 1)

instance Binary (ResponseMessage OffsetFetch 1) where
  get = OffsetFetchResponseV1 <$> get
  put (OffsetFetchResponseV1 r) = put r

instance ByteSize (ResponseMessage OffsetFetch 1) where
  byteSize (OffsetFetchResponseV1 r) = byteSize r 
