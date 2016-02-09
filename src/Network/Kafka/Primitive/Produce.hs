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

makeFields ''PartitionMessages

instance Binary PartitionMessages where
  get = do
    pid <- get
    len <- get
    msgs <- getMessageSet len
    return $ PartitionMessages pid msgs
  put p = do
    putL partition p
    putL (messageSet . to byteSize) p
    putMessageSet $ view messageSet p

instance ByteSize PartitionMessages where
  byteSize p = byteSizeL partition p +
               4 +
               byteSizeL messageSet p


data TopicPublish = TopicPublish
  { topicPublishTopic      :: !Utf8
  , topicPublishPartitions :: !(V.Vector PartitionMessages)
  } deriving (Show, Eq)

makeFields ''TopicPublish

instance Binary TopicPublish where
  get = TopicPublish <$> get <*> (fromArray <$> get)
  put t = do
    putL topic t
    putL (partitions . to Array) t

instance ByteSize TopicPublish where
  byteSize t = byteSizeL topic t +
               byteSizeL partitions t


data ProduceRequestV0 = ProduceRequestV0
  { produceRequestV0RequiredAcks   :: !Int16
  , produceRequestV0Timeout        :: !Int32
  , produceRequestV0TopicPublishes :: !(V.Vector TopicPublish)
  } deriving (Show, Eq, Generic)

makeFields ''ProduceRequestV0

instance Binary ProduceRequestV0 where
  get = ProduceRequestV0 <$> get <*> get <*> (fromArray <$> get)
  put r = do
    putL requiredAcks r
    putL timeout r
    putL (topicPublishes . to Array) r

instance ByteSize ProduceRequestV0 where
  byteSize r = byteSizeL requiredAcks r +
               byteSizeL timeout r +
               byteSizeL topicPublishes r

data PublishPartitionResult = PublishPartitionResult
  { publishPartitionResultPartition :: !PartitionId
  , publishPartitionResultErrorCode :: !ErrorCode
  , publishPartitionResultOffset    :: !Int64
  } deriving (Show, Eq, Generic)

makeFields ''PublishPartitionResult

instance Binary PublishPartitionResult
instance ByteSize PublishPartitionResult where
  byteSize p = byteSizeL partition p +
               byteSizeL errorCode p +
               byteSizeL offset p


data PublishResult = PublishResult
  { publishResultTopic            :: !Utf8
  , publishResultPartitionResults :: !(V.Vector PublishPartitionResult)
  } deriving (Show, Eq, Generic)

makeFields ''PublishResult

instance Binary PublishResult where
  get = PublishResult <$> get <*> (fromArray <$> get)
  put p = do
    putL topic p
    putL (partitionResults . to Array) p

instance ByteSize PublishResult where
  byteSize p = byteSizeL topic p +
               byteSizeL partitionResults p


data ProduceResponseV0 = ProduceResponseV0
  { produceResponseV0Results :: !(V.Vector PublishResult)
  } deriving (Show, Eq, Generic)

makeFields ''ProduceResponseV0

instance Binary ProduceResponseV0 where
  get = (ProduceResponseV0 . fromArray) <$> get
  put r = putL (results . to Array) r

instance ByteSize ProduceResponseV0 where
  byteSize = byteSizeL results

instance RequestApiKey ProduceRequestV0 where
  apiKey = theApiKey 0

instance RequestApiVersion ProduceRequestV0 where
  apiVersion = const 0

