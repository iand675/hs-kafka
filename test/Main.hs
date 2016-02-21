{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Lens hiding (elements)
import qualified Data.Binary as B
import qualified Data.ByteString as BS
import Data.Int
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Network.Kafka
import Network.Kafka.Fields
import Network.Kafka.Simple
import Network.Kafka.Protocol
import Network.Kafka.Primitive.GroupCoordinator  
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
import Test.QuickCheck.Gen

instance Arbitrary ErrorCode where
  arbitrary = Test.QuickCheck.Gen.elements
    [ NoError
    , Unknown
    , OffsetOutOfRange
    , CorruptMessage
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
    , GroupLoadInProgress
    , GroupCoordinatorNotAvailable
    , NotCoordinatorForGroup
    , InvalidTopic
    , RecordListTooLarge
    , NotEnoughReplicas
    , NotEnoughReplicasAfterAppend
    , InvalidRequiredAcks
    , IllegalGeneration
    , InconsistentGroupProtocol
    , InvalidGroupId
    , UnknownMemberId
    , InvalidSessionTimeout
    , RebalanceInProgress
    , InvalidCommitOffsetSize
    , TopicAuthorizationFailed
    , GroupAuthorizationFailed
    , ClusterAuthorizationFailed
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

instance Arbitrary GroupCoordinatorRequestV0 where
  arbitrary = GroupCoordinatorRequestV0 <$> arbitrary

instance Arbitrary GroupCoordinatorResponseV0 where
  arbitrary = GroupCoordinatorResponseV0 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary FetchRequestV0 where
  arbitrary = FetchRequestV0 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary FetchResponseV0 where
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

instance Arbitrary FetchResult where
  arbitrary = FetchResult <$>
              arbitrary <*>
              arbitrary

instance Arbitrary FetchResultPartitionResults where
  arbitrary = FetchResultPartitionResults <$>
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

instance Arbitrary MetadataRequestV0 where
  arbitrary = MetadataRequestV0 <$> arbitrary

instance Arbitrary MetadataResponseV0 where
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

instance Arbitrary OffsetRequestV0 where
  arbitrary = OffsetRequestV0 <$>
              arbitrary <*>
              arbitrary

instance Arbitrary OffsetResponseV0 where
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

instance Arbitrary OffsetCommitRequestV0 where
  arbitrary = OffsetCommitRequestV0 <$>
              arbitrary <*>
              arbitrary

instance Arbitrary OffsetCommitResponseV0 where
  arbitrary = OffsetCommitResponseV0 <$> arbitrary

instance Arbitrary OffsetCommitRequestV1 where
  arbitrary = OffsetCommitRequestV1 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary OffsetCommitResponseV1 where
  arbitrary = OffsetCommitResponseV1 <$> arbitrary

instance Arbitrary OffsetCommitRequestV2 where
  arbitrary = OffsetCommitRequestV2 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary OffsetCommitResponseV2 where
  arbitrary = OffsetCommitResponseV2 <$> arbitrary

instance Arbitrary CommitV0 where
  arbitrary = CommitV0 <$> arbitrary <*> arbitrary

instance Arbitrary CommitPartitionV0 where
  arbitrary = CommitPartitionV0 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary CommitV1 where
  arbitrary = CommitV1 <$> arbitrary <*> arbitrary

instance Arbitrary CommitV2 where
  arbitrary = CommitV2 <$> arbitrary <*> arbitrary

instance Arbitrary CommitTopicResult where
  arbitrary = CommitTopicResult <$> arbitrary <*> arbitrary

instance Arbitrary CommitPartitionV1 where
  arbitrary = CommitPartitionV1 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary CommitPartitionV2 where
  arbitrary = CommitPartitionV2 <$>
              arbitrary <*>
              arbitrary <*>
              arbitrary

instance Arbitrary CommitPartitionResult where
  arbitrary = CommitPartitionResult <$>
              arbitrary <*>
              arbitrary

instance Arbitrary OffsetFetchRequestV0 where
  arbitrary = OffsetFetchRequestV0 <$>
              arbitrary <*>
              arbitrary

instance Arbitrary TopicOffset where
  arbitrary = TopicOffset <$>
              arbitrary <*>
              arbitrary

instance Arbitrary OffsetFetchResponseV0 where
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

instance Arbitrary OffsetFetchRequestV1 where
  arbitrary = OffsetFetchRequestV1 <$>
              arbitrary <*>
              arbitrary

instance Arbitrary OffsetFetchResponseV1 where
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

-- produceAndConsume :: IO 
{-
produceAndConsume = withKafkaConnection "localhost" "9092" $ \conn -> do
  let r = req (CorrelationId 0) (Utf8 "sample") $ ProduceRequestV0 1 0 $
            V.fromList [ TopicPublish (Utf8 "hs-kafka-test-topic") $
                         V.fromList
                         [ PartitionMessages (PartitionId 0) $ MessageSet
                           [ MessageSetItem 0 $ Message 0 (Attributes NoCompression) (Bytes "") (Bytes "hello")
                           ]
                         ]
                       ]
  send conn r
-}

roundTrip :: (Eq a, B.Binary a) => a -> Bool
roundTrip x = x == B.decode (B.encode x)

main = defaultMain $ testGroup "Kafka tests"
  [ {- testGroup "Round trip serialization" 
    [ testGroup "ConsumerMetadata"
      [ testProperty "Request V0" $ \x -> roundTrip (x :: GroupCoordinatorRequestV0)
      , testProperty "Response V0" $ \x -> roundTrip (x :: GroupCoordinatorResponseV0)
      ]
    , testGroup "Fetch"
      [ testProperty "Request V0" $ \x -> roundTrip (x :: FetchRequestV0)
      , testProperty "Response V0" $ \x -> roundTrip (x :: FetchResponseV0)
      ]
    , testGroup "Metadata"
      [ testProperty "Request V0" $ \x -> roundTrip (x :: MetadataRequestV0)
      , testProperty "Response V0" $ \x -> roundTrip (x :: MetadataResponseV0)
      ]
    , testGroup "Offset"
      [ testProperty "Request V0" $ \x -> roundTrip (x :: OffsetRequestV0)
      , testProperty "Response V0" $ \x -> roundTrip (x :: OffsetResponseV0)
      ]
    , testGroup "OffsetCommit"
      [ testProperty "Request V0" $ \x -> roundTrip (x :: OffsetCommitRequestV0)
      , testProperty "Response V0" $ \x -> roundTrip (x :: OffsetCommitResponseV0)
      , testProperty "Request V1" $ \x -> roundTrip (x :: OffsetCommitRequestV1)
      , testProperty "Response V1" $ \x -> roundTrip (x :: OffsetCommitResponseV1)
      , testProperty "Request V2" $ \x -> roundTrip (x :: OffsetCommitRequestV2)
      , testProperty "Response V2" $ \x -> roundTrip (x :: OffsetCommitResponseV2)
      ]
    , testGroup "OffsetFetch"
      [ testProperty "Request V0" $ \x -> roundTrip (x :: OffsetFetchRequestV0)
      , testProperty "Response V0" $ \x -> roundTrip (x :: OffsetFetchResponseV0)
      , testProperty "Request V1" $ \x -> roundTrip (x :: OffsetFetchRequestV1)
      , testProperty "Response V1" $ \x -> roundTrip (x :: OffsetFetchResponseV1)
      ]
    , testGroup "Support types"
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
      ]
    ]
  , -}
    testGroup "Connection" 
    [ "connection open"
    , "connection close"
    , "connection queues requests"
    , "demand result flushes queue"
    ]
  , testGroup "Basic requests return properly"
    [ testCase "groupCoordinator" $ do
        k <- localKafka $ groupCoordinator "test-group"
        assertEqual "error code" NoError $ k ^. errorCode
    {-
    , testCase "fetch" $ do
        localKafka
    , testCase "metadata" $ do
        localKafka
    , testCase "offset" $ do
        localKakfa
    , testCase "offsetCommit" $ do
        localKafka
    , testCase "produce" $ do
        localKafka
    -}
    ]
  , testGroup "Client" []
  , testGroup "Producer" []
  , testGroup "Consumer" []
  ]


