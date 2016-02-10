{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE KindSignatures         #-}
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE TypeFamilies           #-}
module Network.Kafka.Primitive.Fetch where
import           Control.Lens
import qualified Data.Vector as V
import           Data.Binary.Get
import           Network.Kafka.Exports
import           Network.Kafka.Types

data FetchResultPartitionResults = FetchResultPartitionResults
  { fetchResultPartitionResultsPartition           :: !PartitionId
  , fetchResultPartitionResultsErrorCode           :: !ErrorCode
  , fetchResultPartitionResultsHighwaterMarkOffset :: !Int64
  , fetchResultPartitionResultsMessageSet          :: !MessageSet
  } deriving (Show, Eq, Generic)

instance ByteSize FetchResultPartitionResults where
  byteSize r = byteSize (fetchResultPartitionResultsPartition r) +
               byteSize (fetchResultPartitionResultsErrorCode r) +
               byteSize (fetchResultPartitionResultsHighwaterMarkOffset r) +
               byteSize (byteSize $ fetchResultPartitionResultsMessageSet r) +
               byteSize (fetchResultPartitionResultsMessageSet r)

instance Binary FetchResultPartitionResults where
  get = label "fetch result partition results" $ do
    p <- get
    err <- get
    hwmo <- get
    mss <- get
    ms <- getMessageSet mss
    return $ FetchResultPartitionResults p err hwmo ms

  put r = do
    put $ fetchResultPartitionResultsPartition r
    put $ fetchResultPartitionResultsErrorCode r
    put $ fetchResultPartitionResultsHighwaterMarkOffset r
    let c = sum $ map byteSize $ messageSetMessages $ fetchResultPartitionResultsMessageSet r
    put (c :: Int32)
    putMessageSet $ fetchResultPartitionResultsMessageSet r
 
data FetchResult = FetchResult
  { fetchResultTopic            :: !Utf8
  , fetchResultPartitionResults :: !(V.Vector FetchResultPartitionResults)
  } deriving (Show, Eq, Generic)


instance Binary FetchResult where
  get = label "fetch result" $ (FetchResult <$> get <*> (fromArray <$> get))
  put r = do
    put $ fetchResultTopic r
    put $ Array $ fetchResultPartitionResults r

instance ByteSize FetchResult where
  byteSize r = byteSize (fetchResultTopic r) +
               byteSize (fetchResultPartitionResults r)

data PartitionFetch = PartitionFetch
  { partitionFetchPartition :: !PartitionId
  , partitionFetchOffset    :: !Int64
  , partitionFetchMaxBytes  :: !Int32
  } deriving (Show, Eq, Generic)


instance Binary PartitionFetch where
  get = PartitionFetch <$> get <*> get <*> get
  put p = do
    put $ partitionFetchPartition p
    put $ partitionFetchOffset p
    put $ partitionFetchMaxBytes p

instance ByteSize PartitionFetch where
  byteSize p = byteSize (partitionFetchPartition p) +
               byteSize (partitionFetchOffset p) +
               byteSize (partitionFetchMaxBytes p)

data TopicFetch = TopicFetch
  { topicFetchTopic      :: !Utf8
  , topicFetchPartitions :: !(V.Vector PartitionFetch)
  } deriving (Show, Eq, Generic)


instance Binary TopicFetch where
  get = TopicFetch <$> get <*> (fromFixedArray <$> get)
  put t = do
    put $ topicFetchTopic t
    put $ FixedArray $ topicFetchPartitions t

instance ByteSize TopicFetch where
  byteSize t = byteSize (topicFetchTopic t) +
               byteSize (topicFetchPartitions t)

data FetchRequestV0 = FetchRequestV0
  { fetchRequestV0ReplicaId   :: !NodeId
  , fetchRequestV0MaxWaitTime :: !Int32
  , fetchRequestV0MinBytes    :: !Int32
  , fetchRequestV0Topics      :: !(V.Vector TopicFetch)
  } deriving (Show, Eq, Generic)


instance Binary FetchRequestV0 where
  get = label "fetch" (FetchRequestV0 <$> get <*> get <*> get <*> (fromArray <$> get))
  put r = do
    put $ fetchRequestV0ReplicaId r
    put $ fetchRequestV0MaxWaitTime r
    put $ fetchRequestV0MinBytes r
    put $ Array $ fetchRequestV0Topics r

instance ByteSize FetchRequestV0 where
  byteSize r = byteSize (fetchRequestV0ReplicaId r) +
               byteSize (fetchRequestV0MaxWaitTime r) +
               byteSize (fetchRequestV0MinBytes r) +
               byteSize (fetchRequestV0Topics r)

newtype FetchResponseV0 = FetchResponseV0
  { fetchResponseV0_Data :: V.Vector FetchResult
  } deriving (Show, Eq, Generic)


instance Binary FetchResponseV0 where
  get = label "fetch" (FetchResponseV0 <$> (fromArray <$> get))
  put = put . Array . fetchResponseV0_Data

instance ByteSize FetchResponseV0 where
  byteSize (FetchResponseV0 p) = byteSize p
  {-# INLINE byteSize #-}

instance RequestApiKey FetchRequestV0 where
  apiKey = theApiKey 1

instance RequestApiVersion FetchRequestV0 where
  apiVersion = const 0

