{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}
module Network.Kafka.Primitive.Produce where
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data TopicPublish = TopicPublish
  { topicPublishName       :: !Utf8
  , topicPublishPartitions :: !(V.Vector PartitionMessages)
  }

data instance RequestMessage Produce 0 = ProduceRequest
  { produceRequestRequiredAcks   :: !Int16
  , produceRequestTimeout        :: !Int32
  , produceRequestTopicPublishes :: !(V.Vector TopicPublish)
  }

data PublishPartitionResult = PublishPartitionResult
  { publishPartitionResultPartition :: !PartitionId
  , publishPartitionResultErrorCode :: !ErrorCode
  , publishPartitionResultOffset    :: !Int64
  }

data PublishResult = PublishResult
  { publishResultTopicName        :: !Utf8
  , publishResultPartitionResults :: !(V.Vector PublishPartitionResult)
  }

data instance ResponseMessage Produce 0 = ProduceResponse
  { produceResponsePublishResults :: V.Vector PublishResult
  }

