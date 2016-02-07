{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}
module Network.Kafka.Primitive.Fetch where
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data instance RequestMessage Fetch 0 = FetchRequestV0
  { fetchRequestV0ReplicaId   :: !NodeId
  , fetchRequestV0MaxWaitTime :: !Int32
  , fetchRequestV0MinBytes    :: !Int32
  , fetchRequestV0Topics      :: !(V.Vector TopicFetch)
  } deriving (Show, Eq, Generic)

instance Binary (RequestMessage Fetch 0) where
  get = FetchRequestV0 <$> get <*> get <*> get <*> (fromArray <$> get)
  put r = do
    putL replicaId r
    putL maxWaitTime r
    putL minBytes r
    putL (topics . to Array) r

instance ByteSize (RequestMessage Fetch 0) where
  byteSize r = byteSizeL replicaId r +
               byteSizeL maxWaitTime r +
               byteSizeL minBytes r +
               byteSizeL topics r

instance HasReplicaId (RequestMessage Fetch 0) NodeId where
  replicaId = lens fetchRequestV0ReplicaId (\s a -> s { fetchRequestV0ReplicaId = a })
  {-# INLINEABLE replicaId #-}

instance HasMaxWaitTime (RequestMessage Fetch 0) Int32 where
  maxWaitTime = lens fetchRequestV0MaxWaitTime (\s a -> s { fetchRequestV0MaxWaitTime = a })
  {-# INLINEABLE maxWaitTime #-}

instance HasMinBytes (RequestMessage Fetch 0) Int32 where
  minBytes = lens fetchRequestV0MinBytes (\s a -> s { fetchRequestV0MinBytes = a })
  {-# INLINEABLE minBytes #-}

instance HasTopics (RequestMessage Fetch 0) (V.Vector TopicFetch) where
  topics = lens fetchRequestV0Topics (\s a -> s { fetchRequestV0Topics = a })
  {-# INLINEABLE topics #-}


data TopicFetch = TopicFetch
  { topicFetchTopic      :: !Utf8
  , topicFetchPartitions :: !(V.Vector PartitionFetch)
  } deriving (Show, Eq, Generic)

instance Binary TopicFetch where
  get = TopicFetch <$> get <*> (fromFixedArray <$> get)
  put t = putL topic t *> putL (partitions . to FixedArray) t

instance ByteSize TopicFetch where
  byteSize t = byteSizeL topic t + byteSizeL partitions t

instance HasTopic TopicFetch Utf8 where
  topic = lens topicFetchTopic (\s a -> s { topicFetchTopic = a })
  {-# INLINEABLE topic #-}

instance HasPartitions TopicFetch (V.Vector PartitionFetch) where
  partitions = lens topicFetchPartitions (\s a -> s { topicFetchPartitions = a })
  {-# INLINEABLE partitions #-}

data PartitionFetch = PartitionFetch
  { partitionFetchPartition :: !PartitionId
  , partitionFetchOffset    :: !Int64
  , partitionFetchMaxBytes  :: !Int32
  } deriving (Show, Eq, Generic)

instance Binary PartitionFetch where
  get = PartitionFetch <$> get <*> get <*> get
  put p = do
    putL partition p
    putL offset p
    putL maxBytes p

instance ByteSize PartitionFetch where
  byteSize p = byteSizeL partition p +
               byteSizeL offset p +
               byteSizeL maxBytes p

instance HasPartition PartitionFetch PartitionId where
  partition = lens partitionFetchPartition (\s a -> s { partitionFetchPartition = a })
  {-# INLINEABLE partition #-}

instance HasOffset PartitionFetch Int64 where
  offset = lens partitionFetchOffset (\s a -> s { partitionFetchOffset = a })
  {-# INLINEABLE offset #-}

instance HasMaxBytes PartitionFetch Int32 where
  maxBytes = lens partitionFetchMaxBytes (\s a -> s { partitionFetchMaxBytes = a })
  {-# INLINEABLE maxBytes #-}

data instance ResponseMessage Fetch 0 = FetchResponseV0
  { fetchResponseV0Data :: !(V.Vector (FetchResult 0))
  } deriving (Show, Eq, Generic)

instance Binary (ResponseMessage Fetch 0) where
  get = FetchResponseV0 <$> (fromArray <$> get)
  put (FetchResponseV0 p) = put $ Array p

instance ByteSize (ResponseMessage Fetch 0) where
  byteSize (FetchResponseV0 p) = byteSize p
  {-# INLINE byteSize #-}

instance HasData (ResponseMessage Fetch 0) (V.Vector (FetchResult 0)) where
  _data = lens fetchResponseV0Data (\s a -> s { fetchResponseV0Data = a })
  {-# INLINEABLE _data #-}

data family FetchResult (v :: Nat)

data instance FetchResult 0 = FetchResultV0
  { fetchResultV0Partition           :: !PartitionId
  , fetchResultV0ErrorCode           :: !ErrorCode
  , fetchResultV0HighwaterMarkOffset :: !Int64
  , fetchResultV0MessageSet          :: !MessageSet
  } deriving (Show, Eq, Generic)

instance Binary (FetchResult 0) where
  get = do
    p <- get
    err <- get
    hwmo <- get
    mss <- get
    ms <- getMessageSet mss
    return $ FetchResultV0 p err hwmo ms

  put r = do
    putL partition r
    putL errorCode r
    putL highwaterMarkOffset r
    let c = sum $ map byteSize $ messageSetMessages $ fetchResultV0MessageSet r
    put (c :: Int32)
    putMessageSet $ fetchResultV0MessageSet r

instance ByteSize (FetchResult 0) where
  byteSize r = byteSizeL partition r +
               byteSizeL errorCode r +
               byteSizeL highwaterMarkOffset r +
               byteSize (byteSize $ view messageSet r) +
               byteSizeL messageSet r

instance HasPartition (FetchResult 0) PartitionId where
  partition = lens fetchResultV0Partition (\s a -> s { fetchResultV0Partition = a })
  {-# INLINEABLE partition #-}

instance HasErrorCode (FetchResult 0) ErrorCode where
  errorCode = lens fetchResultV0ErrorCode (\s a -> s { fetchResultV0ErrorCode = a })
  {-# INLINEABLE errorCode #-}

instance HasHighwaterMarkOffset (FetchResult 0) Int64 where
  highwaterMarkOffset = lens fetchResultV0HighwaterMarkOffset (\s a -> s { fetchResultV0HighwaterMarkOffset = a })
  {-# INLINEABLE highwaterMarkOffset #-}

instance HasMessageSet (FetchResult 0) MessageSet where
  messageSet = lens fetchResultV0MessageSet (\s a -> s { fetchResultV0MessageSet = a })
  {-# INLINEABLE messageSet #-}
