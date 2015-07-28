{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
module Network.Kafka.Primitive.Fetch where
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data instance RequestMessage Fetch 0 = FetchRequestV0
  { fetchRequestV0ReplicaId   :: !NodeId
  , fetchRequestV0MaxWaitTime :: !Int32
  , fetchRequestV0MinBytes    :: !Int32
  , fetchRequestV0Topic       :: !Utf8
  , fetchRequestV0Partition   :: !PartitionId
  , fetchRequestV0FetchOffset :: !Int64
  , fetchRequestV0MaxBytes    :: !Int32
  }

instance HasReplicaId (RequestMessage Fetch 0) NodeId where
  replicaId = lens fetchRequestV0ReplicaId (\s a -> s { fetchRequestV0ReplicaId = a })
  {-# INLINEABLE replicaId #-}

instance HasMaxWaitTime (RequestMessage Fetch 0) Int32 where
  maxWaitTime = lens fetchRequestV0MaxWaitTime (\s a -> s { fetchRequestV0MaxWaitTime = a })
  {-# INLINEABLE maxWaitTime #-}

instance HasMinBytes (RequestMessage Fetch 0) Int32 where
  minBytes = lens fetchRequestV0MinBytes (\s a -> s { fetchRequestV0MinBytes = a })
  {-# INLINEABLE minBytes #-}

instance HasTopic (RequestMessage Fetch 0) Utf8 where
  topic = lens fetchRequestV0Topic (\s a -> s { fetchRequestV0Topic = a })
  {-# INLINEABLE topic #-}

instance HasPartition (RequestMessage Fetch 0) PartitionId where
  partition = lens fetchRequestV0Partition (\s a -> s { fetchRequestV0Partition = a })
  {-# INLINEABLE partition #-}

instance HasFetchOffset (RequestMessage Fetch 0) Int64 where
  fetchOffset = lens fetchRequestV0FetchOffset (\s a -> s { fetchRequestV0FetchOffset = a })
  {-# INLINEABLE fetchOffset #-}

instance HasMaxBytes (RequestMessage Fetch 0) Int32 where
  maxBytes = lens fetchRequestV0MaxBytes (\s a -> s { fetchRequestV0MaxBytes = a })
  {-# INLINEABLE maxBytes #-}

data instance ResponseMessage Fetch 0 = FetchResponseV0
  { fetchResponseV0Data :: !(V.Vector (FetchResult 0))
  }

instance HasData (ResponseMessage Fetch 0) (V.Vector (FetchResult 0)) where
  _data = lens fetchResponseV0Data (\s a -> s { fetchResponseV0Data = a })
  {-# INLINEABLE _data #-}

data family FetchResult (v :: Nat)

data instance FetchResult 0 = FetchResultV0
  { fetchResultV0Partition           :: !PartitionId
  , fetchResultV0ErrorCode           :: !ErrorCode
  , fetchResultV0HighwaterMarkOffset :: !Int64
  , fetchResultV0MessageSet          :: !MessageSet
  }

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
