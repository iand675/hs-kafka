{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE KindSignatures         #-}
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE TypeFamilies           #-}
module Network.Kafka.Primitive.OffsetFetch where
import           Control.Lens
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types


data PartitionOffsetFetch = PartitionOffsetFetch
  { partitionOffsetFetchPartition :: !PartitionId
  , partitionOffsetFetchOffset    :: !Int64
  , partitionOffsetFetchMetadata  :: !Utf8
  , partitionOffsetFetchErrorCode :: !ErrorCode
  } deriving (Eq, Show, Generic)


instance Binary PartitionOffsetFetch where
  get = PartitionOffsetFetch <$> get <*> get <*> get <*> get
  put p = do
    put $ partitionOffsetFetchPartition p
    put $ partitionOffsetFetchOffset p
    put $ partitionOffsetFetchMetadata p
    put $ partitionOffsetFetchErrorCode p

instance ByteSize PartitionOffsetFetch where
  byteSize p = byteSize (partitionOffsetFetchPartition p) +
               byteSize (partitionOffsetFetchOffset p) +
               byteSize (partitionOffsetFetchMetadata p) +
               byteSize (partitionOffsetFetchErrorCode p)

data TopicOffsetResponse = TopicOffsetResponse
  { topicOffsetResponseTopic      :: !Utf8 
  , topicOffsetResponsePartitions :: !(V.Vector PartitionOffsetFetch)
  } deriving (Eq, Show, Generic)


instance Binary TopicOffsetResponse where
  get = TopicOffsetResponse <$> get <*> (fromArray <$> get)
  put r = do
    put $ topicOffsetResponseTopic r
    put $ Array $ topicOffsetResponsePartitions r

instance ByteSize TopicOffsetResponse where
  byteSize r = byteSize (topicOffsetResponseTopic r) +
               byteSize (topicOffsetResponsePartitions r)


data TopicOffset = TopicOffset
  { topicOffsetTopic      :: !Utf8
  , topicOffsetPartitions :: !(V.Vector PartitionId)
  } deriving (Show, Eq, Generic)


instance Binary TopicOffset where
  get = TopicOffset <$> get <*> (fromFixedArray <$> get)
  put t = do
    put $ topicOffsetTopic t
    put $ FixedArray $ topicOffsetPartitions t

instance ByteSize TopicOffset where
  byteSize t = byteSize (topicOffsetTopic t) +
               byteSize (FixedArray $ topicOffsetPartitions t)

data OffsetFetchRequestV0 = OffsetFetchRequestV0
  { offsetFetchRequestV0ConsumerGroup :: !Utf8
  , offsetFetchRequestV0Topics        :: (V.Vector TopicOffset)
  } deriving (Show, Eq, Generic)


instance Binary OffsetFetchRequestV0 where
  get = OffsetFetchRequestV0 <$> get <*> (fromArray <$> get)
  put o = do
    put (offsetFetchRequestV0ConsumerGroup o)
    put (Array $ offsetFetchRequestV0Topics o)

instance ByteSize OffsetFetchRequestV0 where
  byteSize r = byteSize (offsetFetchRequestV0ConsumerGroup r) +
               byteSize (offsetFetchRequestV0Topics r)

newtype OffsetFetchResponseV0 = OffsetFetchResponseV0
  { offsetFetchResponseV0Topics :: V.Vector TopicOffsetResponse
  } deriving (Show, Eq, Generic)

instance Binary OffsetFetchResponseV0 where
  get = (OffsetFetchResponseV0 . fromArray) <$> get
  put = put . Array . offsetFetchResponseV0Topics


instance ByteSize OffsetFetchResponseV0 where
  byteSize = byteSize . offsetFetchResponseV0Topics


data OffsetFetchRequestV1 = OffsetFetchRequestV1
  { offsetFetchRequestV1ConsumerGroup :: !Utf8
  , offsetFetchRequestV1Topics        :: (V.Vector TopicOffset)
  } deriving (Show, Eq, Generic)


instance Binary OffsetFetchRequestV1 where
  get = OffsetFetchRequestV1 <$> get <*> (fromArray <$> get)
  put (OffsetFetchRequestV1 g t) = put g >> put (Array t)

instance ByteSize OffsetFetchRequestV1 where
  byteSize r = byteSize (offsetFetchRequestV1ConsumerGroup r) +
               byteSize (offsetFetchRequestV1Topics r)

newtype OffsetFetchResponseV1 = OffsetFetchResponseV1
  { offsetFetchResponseV1Topics :: V.Vector TopicOffsetResponse
  } deriving (Show, Eq, Generic)


instance Binary OffsetFetchResponseV1 where
  get = (OffsetFetchResponseV1 . fromArray) <$> get
  put = put . Array . offsetFetchResponseV1Topics

instance ByteSize OffsetFetchResponseV1 where
  byteSize = byteSize . offsetFetchResponseV1Topics

instance RequestApiKey OffsetFetchRequestV0 where
  apiKey = theApiKey 9

instance RequestApiVersion OffsetFetchRequestV0 where
  apiVersion = const 0

instance RequestApiKey OffsetFetchRequestV1 where
  apiKey = theApiKey 9

instance RequestApiVersion OffsetFetchRequestV1 where
  apiVersion = const 1

