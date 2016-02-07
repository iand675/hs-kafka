{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts  #-}
module Main where

import qualified Data.Binary as B
import qualified Data.ByteString as BS
import Data.Int
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Network.Kafka
import Network.Kafka.Primitive.ConsumerMetadata  
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.OffsetFetch
import Network.Kafka.Primitive.Produce
import Network.Kafka.Types
-- import Network.Kafka.Arbitrary
import Test.Tasty
import Test.Tasty.QuickCheck
import Test.Tasty.HUnit

import Test.QuickCheck.Arbitrary

instance Arbitrary ErrorCode where
  arbitrary = elements
    [ NoError
    , Unknown
    , OffsetOutOfRange
    , InvalidMessage
    , UnknownTopicOrPartition
    , InvalidMessageSize
    , LeaderNotAvailable
    , NotLeaderForPartition
    , RequestTimedOut
    , BrokerNotAvailable
    , ReplicaNotAvailable
    , MessageSizeTooLarge
    , StaleControllerEpoch
    , OffsetMetadataTooLarge
    , OffsetsLoadInProgress
    , ConsumerCoordinatorNotAvailable
    , NotCoordinatorForConsumer
    ]

instance Arbitrary Utf8 where
  arbitrary = (Utf8 . T.encodeUtf8 . T.pack) <$> listOf arbitrary

instance Arbitrary CoordinatorId where
  arbitrary = CoordinatorId <$> arbitrary

instance Arbitrary NodeId where
  arbitrary = NodeId <$> arbitrary

instance Arbitrary ConsumerId where
  arbitrary = ConsumerId <$> arbitrary

instance Arbitrary GenerationId where
  arbitrary = GenerationId <$> arbitrary

instance Arbitrary (RequestMessage ConsumerMetadata 0) where
  arbitrary = ConsumerMetadataRequestV0 <$> arbitrary

instance Arbitrary (ResponseMessage ConsumerMetadata 0) where
  arbitrary = ConsumerMetadataResponseV0 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (RequestMessage Fetch 0) where
  arbitrary = FetchRequestV0 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (ResponseMessage Fetch 0) where
  arbitrary = FetchResponseV0 <$>
              arbitrary

instance Arbitrary TopicFetch where
  arbitrary = TopicFetch <$>
              arbitrary <*>
              arbitrary

instance Arbitrary PartitionFetch where
  arbitrary = PartitionFetch <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (FetchResult 0) where
  arbitrary = FetchResultV0 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary MessageSet where
  arbitrary = MessageSet <$> arbitrary

instance Arbitrary MessageSetItem where
  arbitrary = MessageSetItem <$> arbitrary <*> arbitrary

instance Arbitrary Message where
  arbitrary = Message <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary Attributes where
  arbitrary = Attributes <$> arbitrary

instance Arbitrary CompressionCodec where
  arbitrary = elements [NoCompression, GZip, Snappy]

instance Arbitrary a => Arbitrary (V.Vector a) where
  arbitrary = V.fromList <$> listOf arbitrary

instance Arbitrary PartitionId where
  arbitrary = PartitionId <$> arbitrary

instance Arbitrary Bytes where
  arbitrary = (Bytes . BS.pack) <$> listOf arbitrary

instance Arbitrary (RequestMessage Metadata 0) where
  arbitrary = MetadataRequestV0 <$> arbitrary

instance Arbitrary (ResponseMessage Metadata 0) where
  arbitrary = MetadataResponseV0 <$> arbitrary <*> arbitrary

instance Arbitrary Broker where
  arbitrary = Broker <$> arbitrary <*> arbitrary <*> arbitrary

instance Arbitrary TopicMetadata where
  arbitrary = TopicMetadata <$> arbitrary <*> arbitrary <*> arbitrary

instance Arbitrary PartitionMetadata where
  arbitrary = PartitionMetadata <$> 
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (RequestMessage Offset 0) where
  arbitrary = OffsetRequestV0 <$>
              arbitrary <*>
              arbitrary

instance Arbitrary (ResponseMessage Offset 0) where
  arbitrary = OffsetResponseV0 <$> arbitrary

instance Arbitrary TopicPartition where
  arbitrary = TopicPartition <$> arbitrary <*> arbitrary

instance Arbitrary PartitionOffsetRequestInfo where
  arbitrary = PartitionOffsetRequestInfo <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary PartitionOffsetResponseInfo where
  arbitrary = PartitionOffsetResponseInfo <$>
              arbitrary <*>
              arbitrary

instance Arbitrary PartitionOffset where
  arbitrary = PartitionOffset <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (RequestMessage OffsetCommit 0) where
  arbitrary = OffsetCommitRequestV0 <$>
              arbitrary <*>
              arbitrary

instance Arbitrary (ResponseMessage OffsetCommit 0) where
  arbitrary = OffsetCommitResponseV0 <$> arbitrary

instance Arbitrary (RequestMessage OffsetCommit 1) where
  arbitrary = OffsetCommitRequestV1 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (ResponseMessage OffsetCommit 1) where
  arbitrary = OffsetCommitResponseV1 <$> arbitrary

instance Arbitrary (RequestMessage OffsetCommit 2) where
  arbitrary = OffsetCommitRequestV2 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (ResponseMessage OffsetCommit 2) where
  arbitrary = OffsetCommitResponseV2 <$> arbitrary

instance Arbitrary (Commit 0) where
  arbitrary = CommitV0 <$> arbitrary <*> arbitrary

instance Arbitrary (CommitPartition 0) where
  arbitrary = CommitPartitionV0 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (Commit 1) where
  arbitrary = CommitV1 <$> arbitrary <*> arbitrary

instance Arbitrary (Commit 2) where
  arbitrary = CommitV2 <$> arbitrary <*> arbitrary

instance Arbitrary CommitTopicResult where
  arbitrary = CommitTopicResult <$> arbitrary <*> arbitrary

instance Arbitrary (CommitPartition 1) where
  arbitrary = CommitPartitionV1 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (CommitPartition 2) where
  arbitrary = CommitPartitionV2 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary CommitPartitionResult where
  arbitrary = CommitPartitionResult <$>
              arbitrary <*>
              arbitrary

instance Arbitrary (RequestMessage OffsetFetch 0) where
  arbitrary = OffsetFetchRequestV0 <$>
              arbitrary <*>
              arbitrary

instance Arbitrary TopicOffset where
  arbitrary = TopicOffset <$>
              arbitrary <*>
              arbitrary

instance Arbitrary (ResponseMessage OffsetFetch 0) where
  arbitrary = OffsetFetchResponseV0 <$> arbitrary

instance Arbitrary TopicOffsetResponse where
  arbitrary = TopicOffsetResponse <$>
              arbitrary <*>
              arbitrary

instance Arbitrary PartitionOffsetFetch where
  arbitrary = PartitionOffsetFetch <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary (RequestMessage OffsetFetch 1) where
  arbitrary = OffsetFetchRequestV1 <$>
              arbitrary <*>
              arbitrary

instance Arbitrary (ResponseMessage OffsetFetch 1) where
  arbitrary = OffsetFetchResponseV1 <$>
              arbitrary

instance Arbitrary (f a) => Arbitrary (Array f a) where
  arbitrary = Array <$> arbitrary

instance Arbitrary (f a) => Arbitrary (FixedArray f a) where
  arbitrary = FixedArray <$> arbitrary

instance Arbitrary ApiKey where
  arbitrary = ApiKey <$> arbitrary

instance Arbitrary ApiVersion where
  arbitrary = ApiVersion <$> arbitrary

instance Arbitrary CorrelationId where
  arbitrary = CorrelationId <$> arbitrary

roundTrip :: (Eq a, B.Binary a) => a -> Bool
roundTrip x = x == B.decode (B.encode x)

main = defaultMain $ testGroup "Kafka tests"
  [ testGroup "Roundtrip serialization" 
      [ testProperty "Array" $ \x -> roundTrip (x :: Array V.Vector Int16)
      , testProperty "FixedArray" $ \x -> roundTrip (x :: FixedArray V.Vector Int32)
      , testProperty "ErrorCode" $ \x -> roundTrip (x :: ErrorCode)
      , testProperty "ApiKey" $ \x -> roundTrip (x :: ApiKey)
      , testProperty "ApiVersion" $ \x -> roundTrip (x :: ApiVersion)
      , testProperty "CorrelationId" $ \x -> roundTrip (x :: CorrelationId)
      , testProperty "CoordinatorId" $ \x -> roundTrip (x :: CoordinatorId)
      , testProperty "NodeId" $ \x -> roundTrip (x :: NodeId)
      , testProperty "PartitionId" $ \x -> roundTrip (x :: PartitionId)
      , testProperty "Utf8" $ \x -> roundTrip (x :: Utf8)
      , testProperty "Bytes" $ \x -> roundTrip (x :: Bytes)
      , testProperty "Attributes" $ \x -> roundTrip (x :: Attributes)
      , testProperty "RequestMessage ConsumerMetadata 0" $ \x -> roundTrip (x :: RequestMessage ConsumerMetadata 0)
      , testProperty "ResponseMessage ConsumerMetadata 0" $ \x -> roundTrip (x :: ResponseMessage ConsumerMetadata 0)
      , testProperty "RequestMessage Fetch 0" $ \x -> roundTrip (x :: RequestMessage Fetch 0)
      , testProperty "ResponseMessage Fetch 0" $ \x -> roundTrip (x :: ResponseMessage Fetch 0)
      , testProperty "RequestMessage Metadata 0" $ \x -> roundTrip (x :: RequestMessage Metadata 0)
      , testProperty "ResponseMessage Metadata 0" $ \x -> roundTrip (x :: ResponseMessage Metadata 0)
      , testProperty "RequestMessage Offset 0" $ \x -> roundTrip (x :: RequestMessage Offset 0)
      , testProperty "ResponseMessage Offset 0" $ \x -> roundTrip (x :: ResponseMessage Offset 0)
      , testProperty "RequestMessage OffsetCommit 0" $ \x -> roundTrip (x :: RequestMessage OffsetCommit 0)
      , testProperty "ResponseMessage OffsetCommit 0" $ \x -> roundTrip (x :: ResponseMessage OffsetCommit 0)
      , testProperty "RequestMessage OffsetCommit 1" $ \x -> roundTrip (x :: RequestMessage OffsetCommit 1)
      , testProperty "ResponseMessage OffsetCommit 1" $ \x -> roundTrip (x :: ResponseMessage OffsetCommit 1)
      , testProperty "RequestMessage OffsetCommit 2" $ \x -> roundTrip (x :: RequestMessage OffsetCommit 2)
      , testProperty "ResponseMessage OffsetCommit 2" $ \x -> roundTrip (x :: ResponseMessage OffsetCommit 2)
      , testProperty "RequestMessage OffsetFetch 0" $ \x -> roundTrip (x :: RequestMessage OffsetFetch 0)
      , testProperty "ResponseMessage OffsetFetch 0" $ \x -> roundTrip (x :: ResponseMessage OffsetFetch 0)
      , testProperty "RequestMessage OffsetFetch 1" $ \x -> roundTrip (x :: RequestMessage OffsetFetch 1)
      , testProperty "ResponseMessage OffsetFetch 1" $ \x -> roundTrip (x :: ResponseMessage OffsetFetch 1)
      ]
  ]
