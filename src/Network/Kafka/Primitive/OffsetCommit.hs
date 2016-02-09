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

makeFields ''CommitPartitionV0

instance Binary CommitPartitionV0 where
  get = CommitPartitionV0 <$> get <*> get <*> get
  put c = do
    putL partition c
    putL offset c
    putL metadata c

instance ByteSize CommitPartitionV0 where
  byteSize c = byteSizeL partition c +
               byteSizeL offset c +
               byteSizeL metadata c

data CommitV0 = CommitV0
  { commitV0Topic      :: !Utf8
  , commitV0Partitions :: !(V.Vector CommitPartitionV0)
  } deriving (Eq, Show, Generic)


makeFields ''CommitV0

instance Binary CommitV0 where
  get = CommitV0 <$> get <*> (fromArray <$> get)
  put c = do
    putL topic c
    putL (partitions . to Array) c

instance ByteSize CommitV0 where
  byteSize c = byteSizeL topic c +
               byteSizeL partitions c

data OffsetCommitRequestV0 = OffsetCommitRequestV0
  { offsetCommitRequestV0ConsumerGroup :: !Utf8
  , offsetCommitRequestV0Commits       :: !(V.Vector CommitV0)
  } deriving (Eq, Show, Generic)

makeFields ''OffsetCommitRequestV0

instance Binary OffsetCommitRequestV0 where
  get = OffsetCommitRequestV0 <$> get <*> (fromArray <$> get)
  put r = do
    putL consumerGroup r
    putL (commits . to Array) r

instance ByteSize OffsetCommitRequestV0 where
  byteSize r = byteSizeL consumerGroup r +
               byteSizeL commits r


data CommitPartitionV1 = CommitPartitionV1
  { commitPartitionV1Partition :: !PartitionId
  , commitPartitionV1Offset    :: !Int64
  , commitPartitionV1Timestamp :: !Int64
  , commitPartitionV1Metadata  :: !Utf8
  } deriving (Eq, Show, Generic)

makeFields ''CommitPartitionV1

instance Binary CommitPartitionV1 where
  get = CommitPartitionV1 <$> get <*> get <*> get <*> get
  put c = do
    putL partition c
    putL offset c
    putL timestamp c
    putL metadata c

instance ByteSize CommitPartitionV1 where
  byteSize c = byteSizeL partition c +
               byteSizeL offset c +
               byteSizeL timestamp c +
               byteSizeL metadata c

data CommitV1 = CommitV1
  { commitV1Topic      :: !Utf8
  , commitV1Partitions :: !(V.Vector CommitPartitionV1)
  } deriving (Eq, Show, Generic)

makeFields ''CommitV1

instance Binary CommitV1 where
  get = CommitV1 <$> get <*> (fromArray <$> get)
  put c = do
    putL topic c
    putL (partitions . to Array) c

instance ByteSize CommitV1 where
  byteSize c = byteSizeL topic c +
               byteSizeL partitions c

data OffsetCommitRequestV1 = OffsetCommitRequestV1
  { offsetCommitRequestV1ConsumerGroup :: !Utf8
  , offsetCommitRequestV1Generation    :: !GenerationId
  , offsetCommitRequestV1Consumer      :: !ConsumerId
  , offsetCommitRequestV1Commits       :: !(V.Vector CommitV1)
  } deriving (Eq, Show, Generic)

makeFields ''OffsetCommitRequestV1

instance Binary OffsetCommitRequestV1 where
  get = OffsetCommitRequestV1 <$> get <*> get <*> get <*> (fromArray <$> get)
  put r = do
    putL consumerGroup r
    putL generation r
    putL consumer r
    putL (commits . to Array) r

instance ByteSize OffsetCommitRequestV1 where
  byteSize r = byteSizeL consumerGroup r +
               byteSizeL generation r +
               byteSizeL consumer r +
               byteSizeL commits r

data CommitPartitionV2 = CommitPartitionV2
  { commitPartitionV2Partition :: !PartitionId
  , commitPartitionV2Offset    :: !Int64
  , commitPartitionV2Metadata  :: !Utf8
  } deriving (Eq, Show, Generic)

makeFields ''CommitPartitionV2

instance Binary CommitPartitionV2 where
  get = CommitPartitionV2 <$> get <*> get <*> get
  put c = do
    putL partition c
    putL offset c
    putL metadata c

instance ByteSize CommitPartitionV2 where
  byteSize c = byteSizeL partition c +
               byteSizeL offset c +
               byteSizeL metadata c
data CommitV2 = CommitV2
  { commitV2Topic      :: !Utf8
  , commitV2Partitions :: !(V.Vector CommitPartitionV2)
  } deriving (Eq, Show, Generic)

makeFields ''CommitV2

instance Binary CommitV2 where
  get = CommitV2 <$> get <*> (fromArray <$> get)
  put c = do
    putL topic c
    putL (partitions . to Array) c

instance ByteSize CommitV2 where
  byteSize c = byteSizeL topic c +
               byteSizeL partitions c

data OffsetCommitRequestV2 = OffsetCommitRequestV2
  { offsetCommitRequestV2ConsumerGroup :: !Utf8
  , offsetCommitRequestV2Generation    :: !GenerationId
  , offsetCommitRequestV2Consumer      :: !ConsumerId
  , offsetCommitRequestV2RetentionTime :: !Int64
  , offsetCommitRequestV2Commits       :: !(V.Vector CommitV2)
  } deriving (Eq, Show, Generic)

makeFields ''OffsetCommitRequestV2

instance Binary OffsetCommitRequestV2 where
  get = OffsetCommitRequestV2 <$> get <*> get <*> get <*> get <*> (fromArray <$> get)
  put r = do
    putL consumerGroup r
    putL generation r
    putL consumer r
    putL retentionTime r
    putL (commits . to Array) r

instance ByteSize OffsetCommitRequestV2 where
  byteSize r = byteSizeL consumerGroup r +
               byteSizeL generation r +
               byteSizeL consumer r +
               byteSizeL retentionTime r +
               byteSizeL commits r

data CommitPartitionResult = CommitPartitionResult
  { commitPartitionResultPartition :: !PartitionId
  , commitPartitionResultErrorCode :: !ErrorCode
  } deriving (Eq, Show, Generic)

makeFields ''CommitPartitionResult

instance Binary CommitPartitionResult where
  get = CommitPartitionResult <$> get <*> get
  put c = do
    putL partition c
    putL errorCode c

instance ByteSize CommitPartitionResult where
  byteSize c = byteSizeL partition c +
               byteSizeL errorCode c



data CommitTopicResult = CommitTopicResult
  { commitTopicResultTopic   :: !Utf8
  , commitTopicResultResults :: !(V.Vector CommitPartitionResult)
  } deriving (Eq, Show, Generic)

makeFields ''CommitTopicResult

instance Binary CommitTopicResult where
  get = CommitTopicResult <$> get <*> (fromFixedArray <$> get)
  put c = do
    putL topic c
    putL (results . to FixedArray) c

instance ByteSize CommitTopicResult where
  byteSize c = byteSizeL topic c +
               byteSizeL (results . to FixedArray) c

data OffsetCommitResponseV0 = OffsetCommitResponseV0
  { offsetCommitResponseV0Results :: !(V.Vector CommitTopicResult)
  } deriving (Eq, Show, Generic)

makeFields ''OffsetCommitResponseV0

instance Binary OffsetCommitResponseV0 where
  get = (OffsetCommitResponseV0 . fromArray) <$> get
  put = putL (results . to Array)

instance ByteSize OffsetCommitResponseV0 where
  byteSize = byteSizeL results


data OffsetCommitResponseV1 = OffsetCommitResponseV1
  { offsetCommitResponseV1Results :: !(V.Vector CommitTopicResult)
  } deriving (Eq, Show, Generic)

makeFields ''OffsetCommitResponseV1

instance Binary OffsetCommitResponseV1 where
  get = (OffsetCommitResponseV1 . fromArray) <$> get
  put = putL (results . to Array)

instance ByteSize OffsetCommitResponseV1 where
  byteSize = byteSizeL results


data OffsetCommitResponseV2 = OffsetCommitResponseV2
  { offsetCommitResponseV2Results :: !(V.Vector CommitTopicResult)
  } deriving (Eq, Show, Generic)

makeFields ''OffsetCommitResponseV2

instance Binary OffsetCommitResponseV2 where
  get = (OffsetCommitResponseV2 . fromArray) <$> get
  put = putL (results . to Array)

instance ByteSize OffsetCommitResponseV2 where
  byteSize = byteSizeL results

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

