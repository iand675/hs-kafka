{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE TemplateHaskell        #-}
module Network.Kafka.Fields where
import Control.Applicative
import Control.Lens
import Data.Functor.Contravariant
import Data.Profunctor
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.GroupCoordinator
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.OffsetFetch
import Network.Kafka.Primitive.Produce

makeFields ''FetchResultPartitionResults
makeFields ''FetchResult
makeFields ''PartitionFetch
makeFields ''TopicFetch
makeFields ''FetchRequestV0
makeFields ''FetchResponseV0

makeFields ''GroupCoordinatorRequestV0
makeFields ''GroupCoordinatorResponseV0

makeFields ''MetadataRequestV0
makeFields ''Broker
makeFields ''PartitionMetadata
makeFields ''TopicMetadata
makeFields ''MetadataResponseV0

makeFields ''PartitionOffsetRequestInfo
makeFields ''TopicPartition
makeFields ''OffsetRequestV0
makeFields ''PartitionOffset
makeFields ''PartitionOffsetResponseInfo
makeFields ''OffsetResponseV0

makeFields ''CommitPartitionV0
makeFields ''CommitV0
makeFields ''OffsetCommitRequestV0
makeFields ''CommitPartitionV1
makeFields ''CommitV1
makeFields ''OffsetCommitRequestV1
makeFields ''CommitPartitionV2
makeFields ''CommitV2
makeFields ''OffsetCommitRequestV2
makeFields ''CommitPartitionResult
makeFields ''CommitTopicResult
makeFields ''OffsetCommitResponseV0
makeFields ''OffsetCommitResponseV1
makeFields ''OffsetCommitResponseV2

makeFields ''PartitionOffsetFetch
makeFields ''TopicOffsetResponse
makeFields ''TopicOffset
makeFields ''OffsetFetchRequestV0
makeFields ''OffsetFetchResponseV0
makeFields ''OffsetFetchRequestV1
makeFields ''OffsetFetchResponseV1

makeFields ''PartitionMessages
makeFields ''TopicPublish
makeFields ''ProduceRequestV0
makeFields ''PublishPartitionResult
makeFields ''PublishResult
makeFields ''ProduceResponseV0

