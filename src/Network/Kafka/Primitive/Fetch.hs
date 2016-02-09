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

makeFields ''FetchResultPartitionResults

instance ByteSize FetchResultPartitionResults where
  byteSize r = byteSizeL partition r +
               byteSizeL errorCode r +
               byteSizeL highwaterMarkOffset r +
               byteSize (byteSize $ view messageSet r) +
               byteSizeL messageSet r

instance Binary FetchResultPartitionResults where
  get = label "fetch result partition results" $ do
    p <- get
    err <- get
    hwmo <- get
    mss <- get
    ms <- getMessageSet mss
    return $ FetchResultPartitionResults p err hwmo ms

  put r = do
    putL partition r
    putL errorCode r
    putL highwaterMarkOffset r
    let c = sum $ map byteSize $ messageSetMessages $ fetchResultPartitionResultsMessageSet r
    put (c :: Int32)
    putMessageSet $ fetchResultPartitionResultsMessageSet r
 
data FetchResult = FetchResult
  { fetchResultTopic            :: !Utf8
  , fetchResultPartitionResults :: !(V.Vector FetchResultPartitionResults)
  } deriving (Show, Eq, Generic)

makeFields ''FetchResult

instance Binary FetchResult where
  get = label "fetch result" $ (FetchResult <$> get <*> (fromArray <$> get))
  put r = do
    putL topic r
    put (Array $ fetchResultPartitionResults r)

instance ByteSize FetchResult where
  byteSize r = byteSizeL topic r +
               byteSizeL partitionResults r

data PartitionFetch = PartitionFetch
  { partitionFetchPartition :: !PartitionId
  , partitionFetchOffset    :: !Int64
  , partitionFetchMaxBytes  :: !Int32
  } deriving (Show, Eq, Generic)

makeFields ''PartitionFetch

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

data TopicFetch = TopicFetch
  { topicFetchTopic      :: !Utf8
  , topicFetchPartitions :: !(V.Vector PartitionFetch)
  } deriving (Show, Eq, Generic)

makeFields ''TopicFetch

instance Binary TopicFetch where
  get = TopicFetch <$> get <*> (fromFixedArray <$> get)
  put t = putL topic t *> putL (partitions . to FixedArray) t

instance ByteSize TopicFetch where
  byteSize t = byteSizeL topic t + byteSizeL partitions t

data FetchRequestV0 = FetchRequestV0
  { fetchRequestV0ReplicaId   :: !NodeId
  , fetchRequestV0MaxWaitTime :: !Int32
  , fetchRequestV0MinBytes    :: !Int32
  , fetchRequestV0Topics      :: !(V.Vector TopicFetch)
  } deriving (Show, Eq, Generic)

makeFields ''FetchRequestV0


instance Binary FetchRequestV0 where
  get = label "fetch" (FetchRequestV0 <$> get <*> get <*> get <*> (fromArray <$> get))
  put r = do
    putL replicaId r
    putL maxWaitTime r
    putL minBytes r
    putL (topics . to Array) r

instance ByteSize FetchRequestV0 where
  byteSize r = byteSizeL replicaId r +
               byteSizeL maxWaitTime r +
               byteSizeL minBytes r +
               byteSizeL topics r

data FetchResponseV0 = FetchResponseV0
  { fetchResponseV0_Data :: !(V.Vector FetchResult)
  } deriving (Show, Eq, Generic)

makeFields ''FetchResponseV0

instance Binary FetchResponseV0 where
  get = label "fetch" (FetchResponseV0 <$> (fromArray <$> get))
  put (FetchResponseV0 p) = put $ Array p

instance ByteSize FetchResponseV0 where
  byteSize (FetchResponseV0 p) = byteSize p
  {-# INLINE byteSize #-}

instance RequestApiKey FetchRequestV0 where
  apiKey = theApiKey 1

instance RequestApiVersion FetchRequestV0 where
  apiVersion = const 0

