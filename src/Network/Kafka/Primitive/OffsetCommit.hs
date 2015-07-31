{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE TypeFamilies          #-}
module Network.Kafka.Primitive.OffsetCommit where
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data family Commit (v :: Nat)
data family CommitPartition (v :: Nat)



data instance RequestMessage OffsetCommit 0 = OffsetCommitRequestV0
  { offsetCommitRequestV0ConsumerGroup :: !Utf8
  , offsetCommitRequestV0Commits       :: !(V.Vector (Commit 0))
  }

instance Binary (RequestMessage OffsetCommit 0) where
  get = OffsetCommitRequestV0 <$> get <*> (fromArray <$> get)
  put r = do
    putL consumerGroup r
    putL (commits . to Array) r

instance ByteSize (RequestMessage OffsetCommit 0) where
  byteSize r = byteSizeL consumerGroup r +
               byteSizeL commits r

instance HasConsumerGroup (RequestMessage OffsetCommit 0) Utf8 where
  consumerGroup = lens offsetCommitRequestV0ConsumerGroup (\s a -> s { offsetCommitRequestV0ConsumerGroup = a })
  {-# INLINEABLE consumerGroup #-}

instance HasCommits (RequestMessage OffsetCommit 0) (V.Vector (Commit 0)) where
  commits = lens offsetCommitRequestV0Commits (\s a -> s { offsetCommitRequestV0Commits = a })
  {-# INLINEABLE commits #-}



data instance Commit 0 = CommitV0
  { commitV0Topic      :: !Utf8
  , commitV0Partitions :: !(V.Vector (CommitPartition 0))
  }

instance Binary (Commit 0) where
  get = CommitV0 <$> get <*> (fromArray <$> get)
  put c = do
    putL topic c
    putL (partitions . to Array) c

instance ByteSize (Commit 0) where
  byteSize c = byteSizeL topic c +
               byteSizeL partitions c

instance HasTopic (Commit 0) Utf8 where
  topic = lens commitV0Topic (\s a -> s { commitV0Topic = a })
  {-# INLINEABLE topic #-}

instance HasPartitions (Commit 0) (V.Vector (CommitPartition 0)) where
  partitions = lens commitV0Partitions (\s a -> s { commitV0Partitions = a })
  {-# INLINEABLE partitions #-}



data instance CommitPartition 0 = CommitPartitionV0
  { commitPartitionV0Partition :: !PartitionId
  , commitPartitionV0Offset    :: !Int64
  , commitPartitionV0Metadata  :: !Utf8
  }

instance Binary (CommitPartition 0) where
  get = CommitPartitionV0 <$> get <*> get <*> get
  put c = do
    putL partition c
    putL offset c
    putL metadata c

instance ByteSize (CommitPartition 0) where
  byteSize c = byteSizeL partition c +
               byteSizeL offset c +
               byteSizeL metadata c

instance HasPartition (CommitPartition 0) PartitionId where
  partition = lens commitPartitionV0Partition (\s a -> s { commitPartitionV0Partition = a })
  {-# INLINEABLE partition #-}

instance HasOffset (CommitPartition 0) Int64 where
  offset = lens commitPartitionV0Offset (\s a -> s { commitPartitionV0Offset = a })
  {-# INLINEABLE offset #-}

instance HasMetadata (CommitPartition 0) Utf8 where
  metadata = lens commitPartitionV0Metadata (\s a -> s { commitPartitionV0Metadata = a })
  {-# INLINEABLE metadata #-}



data instance RequestMessage OffsetCommit 1 = OffsetCommitRequestV1
  { offsetCommitRequestV1ConsumerGroup :: !Utf8
  , offsetCommitRequestV1Generation    :: !GenerationId
  , offsetCommitRequestV1Consumer      :: !ConsumerId
  , offsetCommitRequestV1Commits       :: !(V.Vector (Commit 1))
  }

instance Binary (RequestMessage OffsetCommit 1) where
  get = OffsetCommitRequestV1 <$> get <*> get <*> get <*> (fromArray <$> get)
  put r = do
    putL consumerGroup r
    putL generation r
    putL consumer r
    putL (commits . to Array) r

instance ByteSize (RequestMessage OffsetCommit 1) where
  byteSize r = byteSizeL consumerGroup r +
               byteSizeL generation r +
               byteSizeL consumer r +
               byteSizeL commits r

instance HasConsumerGroup (RequestMessage OffsetCommit 1) Utf8 where
  consumerGroup = lens offsetCommitRequestV1ConsumerGroup (\s a -> s { offsetCommitRequestV1ConsumerGroup = a })
  {-# INLINEABLE consumerGroup #-}

instance HasGeneration (RequestMessage OffsetCommit 1) GenerationId where
  generation = lens offsetCommitRequestV1Generation (\s a -> s { offsetCommitRequestV1Generation = a })
  {-# INLINEABLE generation #-}

instance HasCommits (RequestMessage OffsetCommit 1) (V.Vector (Commit 1)) where
  commits = lens offsetCommitRequestV1Commits (\s a -> s { offsetCommitRequestV1Commits = a })
  {-# INLINEABLE commits #-}

instance HasConsumer (RequestMessage OffsetCommit 1) ConsumerId where
  consumer = lens offsetCommitRequestV1Consumer (\s a -> s { offsetCommitRequestV1Consumer = a})
  {-# INLINEABLE consumer #-}


data instance Commit 1 = CommitV1
  { commitV1Topic      :: !Utf8
  , commitV1Partitions :: !(V.Vector (CommitPartition 1))
  }

instance Binary (Commit 1) where
  get = CommitV1 <$> get <*> (fromArray <$> get)
  put c = do
    putL topic c
    putL (partitions . to Array) c

instance ByteSize (Commit 1) where
  byteSize c = byteSizeL topic c +
               byteSizeL partitions c

instance HasTopic (Commit 1) Utf8 where
  topic = lens commitV1Topic (\s a -> s { commitV1Topic = a })
  {-# INLINEABLE topic #-}

instance HasPartitions (Commit 1) (V.Vector (CommitPartition 1)) where
  partitions = lens commitV1Partitions (\s a -> s { commitV1Partitions = a })
  {-# INLINEABLE partitions #-}



data instance CommitPartition 1 = CommitPartitionV1
  { commitPartitionV1Partition :: !PartitionId
  , commitPartitionV1Offset    :: !Int64
  , commitPartitionV1Timestamp :: !Int64
  , commitPartitionV1Metadata  :: !Utf8
  }

instance Binary (CommitPartition 1) where
  get = CommitPartitionV1 <$> get <*> get <*> get <*> get
  put c = do
    putL partition c
    putL offset c
    putL timestamp c
    putL metadata c

instance ByteSize (CommitPartition 1) where
  byteSize c = byteSizeL partition c +
               byteSizeL offset c +
               byteSizeL timestamp c +
               byteSizeL metadata c

instance HasPartition (CommitPartition 1) PartitionId where
  partition = lens commitPartitionV1Partition (\s a -> s { commitPartitionV1Partition = a })
  {-# INLINEABLE partition #-}

instance HasOffset (CommitPartition 1) Int64 where
  offset = lens commitPartitionV1Offset (\s a -> s { commitPartitionV1Offset = a })
  {-# INLINEABLE offset #-}

instance HasTimestamp (CommitPartition 1) Int64 where
  timestamp = lens commitPartitionV1Timestamp (\s a -> s { commitPartitionV1Timestamp = a })
  {-# INLINEABLE timestamp #-}

instance HasMetadata (CommitPartition 1) Utf8 where
  metadata = lens commitPartitionV1Metadata (\s a -> s { commitPartitionV1Metadata = a })
  {-# INLINEABLE metadata #-}



data instance RequestMessage OffsetCommit 2 = OffsetCommitRequestV2
  { offsetCommitRequestV2ConsumerGroup :: !Utf8
  , offsetCommitRequestV2Generation    :: !GenerationId
  , offsetCommitRequestV2Consumer      :: !ConsumerId
  , offsetCommitRequestV2RetentionTime :: !Int64
  , offsetCommitRequestV2Commits       :: !(V.Vector (Commit 2))
  }

instance Binary (RequestMessage OffsetCommit 2) where
  get = OffsetCommitRequestV2 <$> get <*> get <*> get <*> get <*> (fromArray <$> get)
  put r = do
    putL consumerGroup r
    putL generation r
    putL consumer r
    putL retentionTime r
    putL (commits . to Array) r

instance ByteSize (RequestMessage OffsetCommit 2) where
  byteSize r = byteSizeL consumerGroup r +
               byteSizeL generation r +
               byteSizeL consumer r +
               byteSizeL retentionTime r +
               byteSizeL commits r

instance HasConsumerGroup (RequestMessage OffsetCommit 2) Utf8 where
  consumerGroup = lens offsetCommitRequestV2ConsumerGroup (\s a -> s { offsetCommitRequestV2ConsumerGroup = a })
  {-# INLINEABLE consumerGroup #-}

instance HasGeneration (RequestMessage OffsetCommit 2) GenerationId where
  generation = lens offsetCommitRequestV2Generation (\s a -> s { offsetCommitRequestV2Generation = a })
  {-# INLINEABLE generation #-}

instance HasCommits (RequestMessage OffsetCommit 2) (V.Vector (Commit 2)) where
  commits = lens offsetCommitRequestV2Commits (\s a -> s { offsetCommitRequestV2Commits = a })
  {-# INLINEABLE commits #-}

instance HasConsumer (RequestMessage OffsetCommit 2) ConsumerId where
  consumer = lens offsetCommitRequestV2Consumer (\s a -> s { offsetCommitRequestV2Consumer = a})
  {-# INLINEABLE consumer #-}

instance HasRetentionTime (RequestMessage OffsetCommit 2) Int64 where
  retentionTime = lens offsetCommitRequestV2RetentionTime (\s a -> s { offsetCommitRequestV2RetentionTime = a })



data instance Commit 2 = CommitV2
  { commitV2Topic      :: !Utf8
  , commitV2Partitions :: !(V.Vector (CommitPartition 2))
  }

instance Binary (Commit 2) where
  get = CommitV2 <$> get <*> (fromArray <$> get)
  put c = do
    putL topic c
    putL (partitions . to Array) c

instance ByteSize (Commit 2) where
  byteSize c = byteSizeL topic c +
               byteSizeL partitions c

instance HasTopic (Commit 2) Utf8 where
  topic = lens commitV2Topic (\s a -> s { commitV2Topic = a })
  {-# INLINEABLE topic #-}

instance HasPartitions (Commit 2) (V.Vector (CommitPartition 2)) where
  partitions = lens commitV2Partitions (\s a -> s { commitV2Partitions = a })
  {-# INLINEABLE partitions #-}



data instance CommitPartition 2 = CommitPartitionV2
  { commitPartitionV2Partition :: !PartitionId
  , commitPartitionV2Offset    :: !Int64
  , commitPartitionV2Metadata  :: !Utf8
  }

instance Binary (CommitPartition 2) where
  get = CommitPartitionV2 <$> get <*> get <*> get
  put c = do
    putL partition c
    putL offset c
    putL metadata c

instance ByteSize (CommitPartition 2) where
  byteSize c = byteSizeL partition c +
               byteSizeL offset c +
               byteSizeL metadata c

instance HasPartition (CommitPartition 2) PartitionId where
  partition = lens commitPartitionV2Partition (\s a -> s { commitPartitionV2Partition = a })
  {-# INLINEABLE partition #-}

instance HasOffset (CommitPartition 2) Int64 where
  offset = lens commitPartitionV2Offset (\s a -> s { commitPartitionV2Offset = a })
  {-# INLINEABLE offset #-}

instance HasMetadata (CommitPartition 2) Utf8 where
  metadata = lens commitPartitionV2Metadata (\s a -> s { commitPartitionV2Metadata = a })
  {-# INLINEABLE metadata #-}



data instance ResponseMessage OffsetCommit 0 = OffsetCommitResponseV0
  { offsetCommitResponseV0Results :: !(V.Vector CommitTopicResult)
  }

instance Binary (ResponseMessage OffsetCommit 0) where
  get = (OffsetCommitResponseV0 . fromArray) <$> get
  put = putL (results . to Array)

instance ByteSize (ResponseMessage OffsetCommit 0) where
  byteSize = byteSizeL results

instance HasResults (ResponseMessage OffsetCommit 0) (V.Vector CommitTopicResult) where
  results = lens offsetCommitResponseV0Results (\s a -> s { offsetCommitResponseV0Results = a })
  {-# INLINEABLE results #-}



data instance ResponseMessage OffsetCommit 1 = OffsetCommitResponseV1
  { offsetCommitResponseV1Results :: !(V.Vector CommitTopicResult)
  }

instance Binary (ResponseMessage OffsetCommit 1) where
  get = (OffsetCommitResponseV1 . fromArray) <$> get
  put = putL (results . to Array)

instance ByteSize (ResponseMessage OffsetCommit 1) where
  byteSize = byteSizeL results

instance HasResults (ResponseMessage OffsetCommit 1) (V.Vector CommitTopicResult) where
  results = lens offsetCommitResponseV1Results (\s a -> s { offsetCommitResponseV1Results = a })
  {-# INLINEABLE results #-}



data instance ResponseMessage OffsetCommit 2 = OffsetCommitResponseV2
  { offsetCommitResponseV2Results :: !(V.Vector CommitTopicResult)
  }

instance Binary (ResponseMessage OffsetCommit 2) where
  get = (OffsetCommitResponseV2 . fromArray) <$> get
  put = putL (results . to Array)

instance ByteSize (ResponseMessage OffsetCommit 2) where
  byteSize = byteSizeL results

instance HasResults (ResponseMessage OffsetCommit 2) (V.Vector CommitTopicResult) where
  results = lens offsetCommitResponseV2Results (\s a -> s { offsetCommitResponseV2Results = a })
  {-# INLINEABLE results #-}



data CommitPartitionResult = CommitPartitionResult
  { commitPartitionResultPartition :: !PartitionId
  , commitPartitionResultErrorCode :: !ErrorCode
  }

instance Binary CommitPartitionResult where
  get = CommitPartitionResult <$> get <*> get
  put c = do
    putL partition c
    putL errorCode c

instance ByteSize CommitPartitionResult where
  byteSize c = byteSizeL partition c +
               byteSizeL errorCode c

instance HasPartition CommitPartitionResult PartitionId where
  partition = lens commitPartitionResultPartition (\s a -> s { commitPartitionResultPartition = a })
  {-# INLINEABLE partition #-}

instance HasErrorCode CommitPartitionResult ErrorCode where
  errorCode = lens commitPartitionResultErrorCode (\s a -> s { commitPartitionResultErrorCode = a })
  {-# INLINEABLE errorCode #-}



data CommitTopicResult = CommitTopicResult
  { commitTopicResultTopic   :: !Utf8
  , commitTopicResultResults :: !(V.Vector CommitPartitionResult)
  }

instance Binary CommitTopicResult where
  get = CommitTopicResult <$> get <*> (fromFixedArray <$> get)
  put c = do
    putL topic c
    putL (results . to FixedArray) c

instance ByteSize CommitTopicResult where
  byteSize c = byteSizeL topic c +
               byteSizeL (results . to FixedArray) c

instance HasTopic CommitTopicResult Utf8 where
  topic = lens commitTopicResultTopic (\s a -> s { commitTopicResultTopic = a })
  {-# INLINEABLE topic #-}

instance HasResults CommitTopicResult (V.Vector CommitPartitionResult) where
  results = lens commitTopicResultResults (\s a -> s { commitTopicResultResults = a })
  {-# INLINEABLE results #-}


