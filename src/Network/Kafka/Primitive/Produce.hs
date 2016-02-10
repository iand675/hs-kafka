{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE KindSignatures         #-}
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE TypeFamilies           #-}
module Network.Kafka.Primitive.Produce where
import qualified Data.Vector as V
import           Control.Lens
import           Network.Kafka.Exports
import           Network.Kafka.Types

data PartitionMessages = PartitionMessages
  { partitionMessagesPartition  :: !PartitionId
  , partitionMessagesMessageSet :: !MessageSet
  } deriving (Show, Eq)


instance Binary PartitionMessages where
  get = do
    pid <- get
    len <- get
    msgs <- getMessageSet len
    return $ PartitionMessages pid msgs
  put p = do
    put $ partitionMessagesPartition p
    put $ byteSize $ partitionMessagesMessageSet p
    putMessageSet $ partitionMessagesMessageSet p

instance ByteSize PartitionMessages where
  byteSize p = byteSize (partitionMessagesPartition p) +
               4 +
               byteSize (partitionMessagesMessageSet p)


data TopicPublish = TopicPublish
  { topicPublishTopic      :: !Utf8
  , topicPublishPartitions :: !(V.Vector PartitionMessages)
  } deriving (Show, Eq)


instance Binary TopicPublish where
  get = TopicPublish <$> get <*> (fromArray <$> get)
  put t = do
    put $ topicPublishTopic t
    put $ Array $ topicPublishPartitions t

instance ByteSize TopicPublish where
  byteSize t = byteSize (topicPublishTopic t) +
               byteSize (topicPublishPartitions t)


data ProduceRequestV0 = ProduceRequestV0
  { produceRequestV0RequiredAcks   :: !Int16
  , produceRequestV0Timeout        :: !Int32
  , produceRequestV0TopicPublishes :: !(V.Vector TopicPublish)
  } deriving (Show, Eq, Generic)


instance Binary ProduceRequestV0 where
  get = ProduceRequestV0 <$> get <*> get <*> (fromArray <$> get)
  put r = do
    put $ produceRequestV0RequiredAcks r
    put $ produceRequestV0Timeout r
    put $ Array $ produceRequestV0TopicPublishes r

instance ByteSize ProduceRequestV0 where
  byteSize r = byteSize (produceRequestV0RequiredAcks r) +
               byteSize (produceRequestV0Timeout r) +
               byteSize (produceRequestV0TopicPublishes r)

data PublishPartitionResult = PublishPartitionResult
  { publishPartitionResultPartition :: !PartitionId
  , publishPartitionResultErrorCode :: !ErrorCode
  , publishPartitionResultOffset    :: !Int64
  } deriving (Show, Eq, Generic)


instance Binary PublishPartitionResult
instance ByteSize PublishPartitionResult where
  byteSize p = byteSize (publishPartitionResultPartition p) +
               byteSize (publishPartitionResultErrorCode p) +
               byteSize (publishPartitionResultOffset p)


data PublishResult = PublishResult
  { publishResultTopic            :: !Utf8
  , publishResultPartitionResults :: !(V.Vector PublishPartitionResult)
  } deriving (Show, Eq, Generic)


instance Binary PublishResult where
  get = PublishResult <$> get <*> (fromArray <$> get)
  put p = do
    put $ publishResultTopic p
    put $ Array $ publishResultPartitionResults p

instance ByteSize PublishResult where
  byteSize p = byteSize (publishResultTopic p) +
               byteSize (publishResultPartitionResults p)


newtype ProduceResponseV0 = ProduceResponseV0
  { produceResponseV0Results :: V.Vector PublishResult
  } deriving (Show, Eq, Generic)


instance Binary ProduceResponseV0 where
  get = (ProduceResponseV0 . fromArray) <$> get
  put = put . Array . produceResponseV0Results

instance ByteSize ProduceResponseV0 where
  byteSize = byteSize . produceResponseV0Results

instance RequestApiKey ProduceRequestV0 where
  apiKey = theApiKey 0

instance RequestApiVersion ProduceRequestV0 where
  apiVersion = const 0

