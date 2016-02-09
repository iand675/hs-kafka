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

makeFields ''PartitionOffsetFetch

instance Binary PartitionOffsetFetch where
  get = PartitionOffsetFetch <$> get <*> get <*> get <*> get
  put p = do
    putL partition p
    putL offset p
    putL metadata p
    putL errorCode p

instance ByteSize PartitionOffsetFetch where
  byteSize p = byteSizeL partition p +
               byteSizeL offset p +
               byteSizeL metadata p +
               byteSizeL errorCode p

data TopicOffsetResponse = TopicOffsetResponse
  { topicOffsetResponseTopic      :: !Utf8 
  , topicOffsetResponsePartitions :: !(V.Vector PartitionOffsetFetch)
  } deriving (Eq, Show, Generic)

makeFields ''TopicOffsetResponse

instance Binary TopicOffsetResponse where
  get = TopicOffsetResponse <$> get <*> (fromArray <$> get)
  put r = do
    putL topic r
    putL (partitions . to Array) r

instance ByteSize TopicOffsetResponse where
  byteSize r = byteSizeL topic r +
               byteSizeL partitions r


data TopicOffset = TopicOffset
  { topicOffsetTopic      :: !Utf8
  , topicOffsetPartitions :: !(V.Vector PartitionId)
  } deriving (Show, Eq, Generic)

makeFields ''TopicOffset

instance Binary TopicOffset where
  get = TopicOffset <$> get <*> (fromFixedArray <$> get)
  put t = do
    putL topic t
    putL (partitions . to FixedArray) t

instance ByteSize TopicOffset where
  byteSize t = byteSizeL topic t + byteSizeL (partitions . to FixedArray) t

data OffsetFetchRequestV0 = OffsetFetchRequestV0
  { offsetFetchRequestV0ConsumerGroup :: !Utf8
  , offsetFetchRequestV0Topics        :: (V.Vector TopicOffset)
  } deriving (Show, Eq, Generic)

makeFields ''OffsetFetchRequestV0

instance Binary OffsetFetchRequestV0 where
  get = OffsetFetchRequestV0 <$> get <*> (fromArray <$> get)
  put o = do
    putL consumerGroup o
    putL (topics . to Array) o

instance ByteSize OffsetFetchRequestV0 where
  byteSize r = byteSizeL consumerGroup r + byteSizeL topics r

data OffsetFetchResponseV0 = OffsetFetchResponseV0
  { offsetFetchResponseV0Topics :: !(V.Vector TopicOffsetResponse)
  } deriving (Show, Eq, Generic)

instance Binary OffsetFetchResponseV0 where
  get = (OffsetFetchResponseV0 . fromArray) <$> get
  put r = putL (topics . to Array) r

makeFields ''OffsetFetchResponseV0

instance ByteSize OffsetFetchResponseV0 where
  byteSize r = byteSizeL topics r


data OffsetFetchRequestV1 = OffsetFetchRequestV1
  { offsetFetchRequestV1ConsumerGroup :: !Utf8
  , offsetFetchRequestV1Topics        :: (V.Vector TopicOffset)
  } deriving (Show, Eq, Generic)

makeFields ''OffsetFetchRequestV1

instance Binary OffsetFetchRequestV1 where
  get = OffsetFetchRequestV1 <$> get <*> (fromArray <$> get)
  put (OffsetFetchRequestV1 g t) = put g >> put (Array t)

instance ByteSize OffsetFetchRequestV1 where
  byteSize r = byteSizeL consumerGroup r + byteSizeL topics r

data OffsetFetchResponseV1 = OffsetFetchResponseV1
  { offsetFetchResponseV1Topics :: !(V.Vector TopicOffsetResponse)
  } deriving (Show, Eq, Generic)

makeFields ''OffsetFetchResponseV1

instance Binary OffsetFetchResponseV1 where
  get = (OffsetFetchResponseV1 . fromArray) <$> get
  put r = putL (topics . to Array) r

instance ByteSize OffsetFetchResponseV1 where
  byteSize r = byteSizeL topics r

instance RequestApiKey OffsetFetchRequestV0 where
  apiKey = theApiKey 9

instance RequestApiVersion OffsetFetchRequestV0 where
  apiVersion = const 0

instance RequestApiKey OffsetFetchRequestV1 where
  apiKey = theApiKey 9

instance RequestApiVersion OffsetFetchRequestV1 where
  apiVersion = const 1

