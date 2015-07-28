{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
module Network.Kafka.Primitive.OffsetCommit where
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data family Commit (v :: Nat)
data family CommitPartition (v :: Nat)

data instance RequestMessage OffsetCommit 0 = OffsetCommitRequestV0
  { offsetCommitRequestV0ConsumerGroupId :: !Utf8
  , offsetCommitRequestV0Commits         :: !(V.Vector (Commit 0))
  }

data instance Commit 0 = CommitV0
  { commitV0Topic      :: !Utf8
  , commitV0Partitions :: !(V.Vector (CommitPartition 0))
  }

data instance CommitPartition 0 = CommitPartitionV0
  { commitPartitionV0Partition :: !PartitionId
  , commitPartitionV0Offset    :: !Int64
  , commitPartitionV0Metadata  :: !Utf8
  }

data instance RequestMessage OffsetCommit 1 = OffsetCommitRequestV1
  { offsetCommitRequestV1ConsumerGroupId           :: !Utf8
  , offsetCommitRequestV1ConsumerGroupGenerationId :: !GenerationId
  , offsetCommitRequestV1ConsumerId                :: !ConsumerId
  , offsetCommitRequestV1Commits                   :: !(V.Vector (Commit 1))
  }

data instance Commit 1 = CommitV1
  { commitV1Topic      :: !Utf8
  , commitV1Partitions :: !(V.Vector (CommitPartition 1))
  }

data instance CommitPartition 1 = CommitPartitionV1
  { commitPartitionV1Partition :: !PartitionId
  , commitPartitionV1Offset    :: !Int64
  , commitPartitionV1Timestamp :: !Int64
  , commitPartitionV1Metadata  :: !Utf8
  }

data instance RequestMessage OffsetCommit 2 = OffsetCommitRequest
  { offsetCommitRequestV2ConsumerGroupId           :: !Utf8
  , offsetCommitRequestV2ConsumerGroupGenerationId :: !GenerationId
  , offsetCommitRequestV2ConsumerId                :: !ConsumerId
  , offsetCommitRequestV2RetentionTime             :: !Int64
  , offsetCommitRequestV2Commits                   :: !(V.Vector (Commit 2))
  }

data instance Commit 2 = CommitV2
  { commitV2Topic      :: !Utf8
  , commitV2Partitions :: !(V.Vector (CommitPartition 2))
  }

data instance CommitPartition 2 = CommitPartitionV2
  { commitPartitionV2Partition :: !PartitionId
  , commitPartitionV2Offset    :: !Int64
  , commitPartitionV2Metadata  :: !Utf8
  }


data instance ResponseMessage OffsetCommit 0 = OffsetCommitResponseV0
  { offsetCommitResponseV0Results :: !(V.Vector CommitTopicResult)
  }

data instance ResponseMessage OffsetCommit 1 = OffsetCommitResponseV1
  { offsetCommitResponseV1Results :: !(V.Vector CommitTopicResult)
  }

data instance ResponseMessage OffsetCommit 2 = OffsetCommitResponseV2
  { offsetCommitResponseV2Results :: !(V.Vector CommitTopicResult)
  }

data CommitPartitionResult = CommitPartitionResult
  { commitPartitionResultPartition :: !PartitionId
  , commitPartitionResultErrorCode :: !ErrorCode
  }

data CommitTopicResult = CommitTopicResult
  { commitTopicResultTopic            :: !Utf8
  , commitTopicResultPartitionResults :: !(V.Vector CommitPartitionResult)
  }
