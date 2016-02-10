{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE KindSignatures         #-}
{-# LANGUAGE TypeFamilies           #-}
module Network.Kafka.Primitive.OffsetCommit where
import           Control.Lens
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data CommitPartitionV0 = CommitPartitionV0
  { commitPartitionV0Partition :: !PartitionId
  , commitPartitionV0Offset    :: !Int64
  , commitPartitionV0Metadata  :: !Utf8
  } deriving (Eq, Show, Generic)


instance Binary CommitPartitionV0 where
  get = CommitPartitionV0 <$> get <*> get <*> get
  put c = do
    put $ commitPartitionV0Partition c
    put $ commitPartitionV0Offset c
    put $ commitPartitionV0Metadata c

instance ByteSize CommitPartitionV0 where
  byteSize c = byteSize (commitPartitionV0Partition c) +
               byteSize (commitPartitionV0Offset c) +
               byteSize (commitPartitionV0Metadata c)

data CommitV0 = CommitV0
  { commitV0Topic      :: !Utf8
  , commitV0Partitions :: !(V.Vector CommitPartitionV0)
  } deriving (Eq, Show, Generic)


instance Binary CommitV0 where
  get = CommitV0 <$> get <*> (fromArray <$> get)
  put c = do
    put $ commitV0Topic c
    put $ Array $ commitV0Partitions c

instance ByteSize CommitV0 where
  byteSize c = byteSize (commitV0Topic c) +
               byteSize (commitV0Partitions c)

data OffsetCommitRequestV0 = OffsetCommitRequestV0
  { offsetCommitRequestV0ConsumerGroup :: !Utf8
  , offsetCommitRequestV0Commits       :: !(V.Vector CommitV0)
  } deriving (Eq, Show, Generic)


instance Binary OffsetCommitRequestV0 where
  get = OffsetCommitRequestV0 <$> get <*> (fromArray <$> get)
  put r = do
    put $ offsetCommitRequestV0ConsumerGroup r
    put $ Array $ offsetCommitRequestV0Commits r

instance ByteSize OffsetCommitRequestV0 where
  byteSize r = byteSize (offsetCommitRequestV0ConsumerGroup r) +
               byteSize (offsetCommitRequestV0Commits r)


data CommitPartitionV1 = CommitPartitionV1
  { commitPartitionV1Partition :: !PartitionId
  , commitPartitionV1Offset    :: !Int64
  , commitPartitionV1Timestamp :: !Int64
  , commitPartitionV1Metadata  :: !Utf8
  } deriving (Eq, Show, Generic)


instance Binary CommitPartitionV1 where
  get = CommitPartitionV1 <$> get <*> get <*> get <*> get
  put c = do
    put $ commitPartitionV1Partition c
    put $ commitPartitionV1Offset c
    put $ commitPartitionV1Timestamp c
    put $ commitPartitionV1Metadata c

instance ByteSize CommitPartitionV1 where
  byteSize c = byteSize (commitPartitionV1Partition c) +
               byteSize (commitPartitionV1Offset c) +
               byteSize (commitPartitionV1Timestamp c) +
               byteSize (commitPartitionV1Metadata c)

data CommitV1 = CommitV1
  { commitV1Topic      :: !Utf8
  , commitV1Partitions :: !(V.Vector CommitPartitionV1)
  } deriving (Eq, Show, Generic)

instance Binary CommitV1 where
  get = CommitV1 <$> get <*> (fromArray <$> get)
  put c = do
    put $ commitV1Topic c
    put $ Array $ commitV1Partitions c

instance ByteSize CommitV1 where
  byteSize c = byteSize (commitV1Topic c) +
               byteSize (commitV1Partitions c)

data OffsetCommitRequestV1 = OffsetCommitRequestV1
  { offsetCommitRequestV1ConsumerGroup :: !Utf8
  , offsetCommitRequestV1Generation    :: !GenerationId
  , offsetCommitRequestV1Consumer      :: !ConsumerId
  , offsetCommitRequestV1Commits       :: !(V.Vector CommitV1)
  } deriving (Eq, Show, Generic)


instance Binary OffsetCommitRequestV1 where
  get = OffsetCommitRequestV1 <$> get <*> get <*> get <*> (fromArray <$> get)
  put r = do
    put $ offsetCommitRequestV1ConsumerGroup r
    put $ offsetCommitRequestV1Generation r
    put $ offsetCommitRequestV1Consumer r
    put $ Array $ offsetCommitRequestV1Commits r

instance ByteSize OffsetCommitRequestV1 where
  byteSize r = byteSize (offsetCommitRequestV1ConsumerGroup r) +
               byteSize (offsetCommitRequestV1Generation r) +
               byteSize (offsetCommitRequestV1Consumer r) +
               byteSize (offsetCommitRequestV1Commits r)

data CommitPartitionV2 = CommitPartitionV2
  { commitPartitionV2Partition :: !PartitionId
  , commitPartitionV2Offset    :: !Int64
  , commitPartitionV2Metadata  :: !Utf8
  } deriving (Eq, Show, Generic)


instance Binary CommitPartitionV2 where
  get = CommitPartitionV2 <$> get <*> get <*> get
  put c = do
    put $ commitPartitionV2Partition c
    put $ commitPartitionV2Offset c
    put $ commitPartitionV2Metadata c

instance ByteSize CommitPartitionV2 where
  byteSize c = byteSize (commitPartitionV2Partition c) +
               byteSize (commitPartitionV2Offset c) +
               byteSize (commitPartitionV2Metadata c)

data CommitV2 = CommitV2
  { commitV2Topic      :: !Utf8
  , commitV2Partitions :: !(V.Vector CommitPartitionV2)
  } deriving (Eq, Show, Generic)

instance Binary CommitV2 where
  get = CommitV2 <$> get <*> (fromArray <$> get)
  put c = do
    put $ commitV2Topic c
    put $ Array $ commitV2Partitions c

instance ByteSize CommitV2 where
  byteSize c = byteSize (commitV2Topic c) +
               byteSize (commitV2Partitions c)

data OffsetCommitRequestV2 = OffsetCommitRequestV2
  { offsetCommitRequestV2ConsumerGroup :: !Utf8
  , offsetCommitRequestV2Generation    :: !GenerationId
  , offsetCommitRequestV2Consumer      :: !ConsumerId
  , offsetCommitRequestV2RetentionTime :: !Int64
  , offsetCommitRequestV2Commits       :: !(V.Vector CommitV2)
  } deriving (Eq, Show, Generic)


instance Binary OffsetCommitRequestV2 where
  get = OffsetCommitRequestV2 <$> get <*> get <*> get <*> get <*> (fromArray <$> get)
  put r = do
    put $ offsetCommitRequestV2ConsumerGroup r
    put $ offsetCommitRequestV2Generation r
    put $ offsetCommitRequestV2Consumer r
    put $ offsetCommitRequestV2RetentionTime r
    put $ Array $ offsetCommitRequestV2Commits r

instance ByteSize OffsetCommitRequestV2 where
  byteSize r = byteSize (offsetCommitRequestV2ConsumerGroup r) +
               byteSize (offsetCommitRequestV2Generation r) +
               byteSize (offsetCommitRequestV2Consumer r) +
               byteSize (offsetCommitRequestV2RetentionTime r) +
               byteSize (offsetCommitRequestV2Commits r)

data CommitPartitionResult = CommitPartitionResult
  { commitPartitionResultPartition :: !PartitionId
  , commitPartitionResultErrorCode :: !ErrorCode
  } deriving (Eq, Show, Generic)


instance Binary CommitPartitionResult where
  get = CommitPartitionResult <$> get <*> get
  put c = do
    put $ commitPartitionResultPartition c
    put $ commitPartitionResultErrorCode c

instance ByteSize CommitPartitionResult where
  byteSize c = byteSize (commitPartitionResultPartition c) +
               byteSize (commitPartitionResultErrorCode c)


data CommitTopicResult = CommitTopicResult
  { commitTopicResultTopic   :: !Utf8
  , commitTopicResultResults :: !(V.Vector CommitPartitionResult)
  } deriving (Eq, Show, Generic)


instance Binary CommitTopicResult where
  get = CommitTopicResult <$> get <*> (fromFixedArray <$> get)
  put c = do
    put $ commitTopicResultTopic c
    put $ FixedArray $ commitTopicResultResults c

instance ByteSize CommitTopicResult where
  byteSize c = byteSize (commitTopicResultTopic c) +
               byteSize (FixedArray $ commitTopicResultResults c)

newtype OffsetCommitResponseV0 = OffsetCommitResponseV0
  { offsetCommitResponseV0Results :: V.Vector CommitTopicResult
  } deriving (Eq, Show, Generic)


instance Binary OffsetCommitResponseV0 where
  get = (OffsetCommitResponseV0 . fromArray) <$> get
  put = put . Array . offsetCommitResponseV0Results

instance ByteSize OffsetCommitResponseV0 where
  byteSize = byteSize . offsetCommitResponseV0Results


newtype OffsetCommitResponseV1 = OffsetCommitResponseV1
  { offsetCommitResponseV1Results :: V.Vector CommitTopicResult
  } deriving (Eq, Show, Generic)


instance Binary OffsetCommitResponseV1 where
  get = (OffsetCommitResponseV1 . fromArray) <$> get
  put = put . Array . offsetCommitResponseV1Results

instance ByteSize OffsetCommitResponseV1 where
  byteSize = byteSize . offsetCommitResponseV1Results


newtype OffsetCommitResponseV2 = OffsetCommitResponseV2
  { offsetCommitResponseV2Results :: V.Vector CommitTopicResult
  } deriving (Eq, Show, Generic)


instance Binary OffsetCommitResponseV2 where
  get = (OffsetCommitResponseV2 . fromArray) <$> get
  put = put . Array . offsetCommitResponseV2Results

instance ByteSize OffsetCommitResponseV2 where
  byteSize = byteSize . offsetCommitResponseV2Results

instance RequestApiKey OffsetCommitRequestV0 where
  apiKey = theApiKey 8

instance RequestApiVersion OffsetCommitRequestV0 where
  apiVersion = const 0

instance RequestApiKey OffsetCommitRequestV1 where
  apiKey = theApiKey 8

instance RequestApiVersion OffsetCommitRequestV1 where
  apiVersion = const 1

instance RequestApiKey OffsetCommitRequestV2 where
  apiKey = theApiKey 8

instance RequestApiVersion OffsetCommitRequestV2 where
  apiVersion = const 2

