{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Network.Kafka.Primitive
  ( fetch
  , groupCoordinator
  , metadata
  , offset
  , offsetCommit
  , offsetFetch
  , produce
  ) where
import Control.Exception
import Control.Concurrent.MVar
import Control.Lens
import Control.Monad.Trans
import Control.Monad.Trans.State
import Data.Int
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Network.Kafka.Internal.Connection
import Network.Kafka.Protocol
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.GroupCoordinator
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.OffsetFetch
import Network.Kafka.Primitive.Produce
import Network.Kafka.Types hiding (offset)

localKafka :: (KafkaContext -> IO a) -> IO a
localKafka f = withKafkaConnection "localhost" "9092" defaultConfig $ \c ->
  f (KafkaContext c defaultConfig)

internal :: KafkaAction req resp => KafkaContext -> req -> IO resp
internal c = send (kafkaContextConnection c) (kafkaContextConfig c)

groupCoordinator :: KafkaContext -> T.Text -> IO GroupCoordinatorResponseV0
groupCoordinator c t = internal c $ GroupCoordinatorRequestV0 $ Utf8 (T.encodeUtf8 t)

fetch :: KafkaContext -> V.Vector TopicFetch -> IO (V.Vector FetchResult)
fetch ctxt fs = do
  let c = kafkaContextConfig ctxt
  fetchResponseV0_Data <$> internal ctxt (FetchRequestV0 (NodeId (-1)) -- -1 is a specific value for non-brokers
                                                         (fromIntegral $ c ^. fetchMaxWaitTime)
                                                         (fromIntegral $ c ^. fetchMinBytes)
                                                         fs)

metadata :: KafkaContext -> V.Vector Utf8 -> IO MetadataResponseV0
metadata c = internal c . MetadataRequestV0

offset :: KafkaContext -> NodeId -> V.Vector TopicPartition -> IO (V.Vector PartitionOffsetResponseInfo)
offset c n ps = offsetResponseV0Offsets <$> internal c (OffsetRequestV0 n ps)

offsetCommit :: KafkaContext -> OffsetCommitRequestV2 -> IO (V.Vector CommitTopicResult)
offsetCommit c = fmap offsetCommitResponseV2Results . internal c

offsetFetch :: KafkaContext
            -> T.Text
            -> V.Vector TopicOffset
            -> IO (V.Vector TopicOffsetResponse)
offsetFetch c cg ts = do
  let g = Utf8 $ T.encodeUtf8 cg
  offsetFetchResponseV1Topics <$> internal c (OffsetFetchRequestV1 g ts)

produce :: KafkaContext -> V.Vector TopicPublish -> IO (V.Vector PublishResult)
produce ctxt ts = do
  let c = kafkaContextConfig ctxt
      req = ProduceRequestV0
              { produceRequestV0RequiredAcks = fromIntegral $ fromEnum $ c ^. acks
              , produceRequestV0Timeout = fromIntegral $ c ^. timeoutMs
              , produceRequestV0TopicPublishes = ts
              }
  produceResponseV0Results <$> internal ctxt req

