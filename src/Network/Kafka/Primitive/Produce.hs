{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}
module Network.Kafka.Primitive.Produce where
import qualified Data.Vector as V
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
    putL partition p
    putL (messageSet . to byteSize) p
    putMessageSet $ view messageSet p

instance ByteSize PartitionMessages where
  byteSize p = byteSizeL partition p +
               4 +
               byteSizeL messageSet p

instance HasPartition PartitionMessages PartitionId where
  partition = lens partitionMessagesPartition (\s a -> s { partitionMessagesPartition = a })
  {-# INLINEABLE partition #-}

instance HasMessageSet PartitionMessages MessageSet where
  messageSet = lens partitionMessagesMessageSet (\s a -> s { partitionMessagesMessageSet = a })
  {-# INLINEABLE messageSet #-}



data TopicPublish = TopicPublish
  { topicPublishTopic      :: !Utf8
  , topicPublishPartitions :: !(V.Vector PartitionMessages)
  } deriving (Show, Eq)

instance Binary TopicPublish where
  get = TopicPublish <$> get <*> (fromArray <$> get)
  put t = do
    putL topic t
    putL (partitions . to Array) t

instance ByteSize TopicPublish where
  byteSize t = byteSizeL topic t +
               byteSizeL partitions t

instance HasTopic TopicPublish Utf8 where
   topic = lens topicPublishTopic (\s a -> s { topicPublishTopic = a })
   {-# INLINEABLE topic #-}

instance HasPartitions TopicPublish (V.Vector PartitionMessages) where
   partitions = lens topicPublishPartitions (\s a -> s { topicPublishPartitions = a })
   {-# INLINEABLE partitions #-}



data instance RequestMessage Produce 0 = ProduceRequestV0
  { produceRequestV0RequiredAcks   :: !Int16
  , produceRequestV0Timeout        :: !Int32
  , produceRequestV0TopicPublishes :: !(V.Vector TopicPublish)
  } deriving (Show, Eq)

instance Binary (RequestMessage Produce 0) where
  get = ProduceRequestV0 <$> get <*> get <*> (fromArray <$> get)
  put r = do
    putL requiredAcks r
    putL timeout r
    putL (topicPublishes . to Array) r

instance ByteSize (RequestMessage Produce 0) where
  byteSize r = byteSizeL requiredAcks r +
               byteSizeL timeout r +
               byteSizeL topicPublishes r

instance HasRequiredAcks (RequestMessage Produce 0) Int16 where
  requiredAcks = lens produceRequestV0RequiredAcks (\s a -> s { produceRequestV0RequiredAcks = a })
  {-# INLINEABLE requiredAcks #-}

instance HasTimeout (RequestMessage Produce 0) Int32 where
  timeout = lens produceRequestV0Timeout (\s a -> s { produceRequestV0Timeout = a })
  {-# INLINEABLE timeout #-}

instance HasTopicPublishes (RequestMessage Produce 0) (V.Vector TopicPublish) where
  topicPublishes = lens produceRequestV0TopicPublishes (\s a -> s { produceRequestV0TopicPublishes = a })
  {-# INLINEABLE topicPublishes #-}



data PublishPartitionResult = PublishPartitionResult
  { publishPartitionResultPartition :: !PartitionId
  , publishPartitionResultErrorCode :: !ErrorCode
  , publishPartitionResultOffset    :: !Int64
  } deriving (Generic)

instance Binary PublishPartitionResult
instance ByteSize PublishPartitionResult where
  byteSize p = byteSizeL partition p +
               byteSizeL errorCode p +
               byteSizeL offset p

instance HasPartition PublishPartitionResult PartitionId where
  partition = lens publishPartitionResultPartition (\s a -> s { publishPartitionResultPartition = a })
  {-# INLINEABLE partition #-}

instance HasErrorCode PublishPartitionResult ErrorCode where
  errorCode = lens publishPartitionResultErrorCode (\s a -> s { publishPartitionResultErrorCode = a })
  {-# INLINEABLE errorCode #-}

instance HasOffset PublishPartitionResult Int64 where
  offset = lens publishPartitionResultOffset (\s a -> s { publishPartitionResultOffset = a })
  {-# INLINEABLE offset #-}



data PublishResult = PublishResult
  { publishResultTopic            :: !Utf8
  , publishResultPartitionResults :: !(V.Vector PublishPartitionResult)
  }

instance Binary PublishResult where
  get = PublishResult <$> get <*> (fromArray <$> get)
  put p = do
    putL topic p
    putL (partitionResults . to Array) p

instance ByteSize PublishResult where
  byteSize p = byteSizeL topic p +
               byteSizeL partitionResults p

instance HasTopic PublishResult Utf8 where
  topic = lens publishResultTopic (\s a -> s { publishResultTopic = a })
  {-# INLINEABLE topic #-}

instance HasPartitionResults PublishResult (V.Vector PublishPartitionResult) where
  partitionResults = lens publishResultPartitionResults (\s a -> s { publishResultPartitionResults = a })
  {-# INLINEABLE partitionResults #-}



data instance ResponseMessage Produce 0 = ProduceResponseV0
  { produceResponseV0Results :: !(V.Vector PublishResult)
  }

instance Binary (ResponseMessage Produce 0) where
  get = (ProduceResponseV0 . fromArray) <$> get
  put r = putL (results . to Array) r

instance ByteSize (ResponseMessage Produce 0) where
  byteSize = byteSizeL results

instance HasResults (ResponseMessage Produce 0) (V.Vector PublishResult) where
  results = lens produceResponseV0Results (\s a -> s { produceResponseV0Results = a })
  {-# INLINEABLE results #-}

