{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Network.Kafka.Simple where
import Control.Monad.Trans
import Control.Monad.Trans.State
import Data.Int
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Network.Kafka.Protocol
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.GroupCoordinator
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.OffsetFetch
import Network.Kafka.Primitive.Produce
import Network.Kafka.Types

localKafka :: Kafka a -> IO a
localKafka k = withKafkaConnection "localhost" "9092" defaultConfig $ \c ->
  runKafka (KafkaContext c defaultConfig) k

runKafka :: KafkaContext -> Kafka a -> IO a
runKafka c (Kafka m) = evalStateT m c

internal :: KafkaAction req resp => req -> Kafka resp
internal r = Kafka $ do
  c <- get
  liftIO $ send (kafkaContextConnection c) (kafkaContextConfig c) r

groupCoordinator :: T.Text -> Kafka GroupCoordinatorResponseV0
groupCoordinator t = internal $ GroupCoordinatorRequestV0 $ Utf8 (T.encodeUtf8 t)

fetch :: V.Vector TopicFetch -> Kafka (V.Vector FetchResult)
fetch fs = do
  c <- kafkaContextConfig <$> Kafka get
  fetchResponseV0_Data <$> internal (FetchRequestV0 (NodeId (-1)) -- -1 is a specific value for non-brokers
                                                    (fromIntegral $ kafkaConfigFetchMaxWaitTime c)
                                                    (fromIntegral $ kafkaConfigFetchMinBytes c)
                                                    fs)

metadata :: V.Vector T.Text -> Kafka MetadataResponseV0
metadata = internal . MetadataRequestV0 . V.map (Utf8 . T.encodeUtf8)

offset :: OffsetRequestV0 -> Kafka (V.Vector PartitionOffsetResponseInfo)
offset = fmap offsetResponseV0Offsets . internal

offsetCommit :: OffsetCommitRequestV2 -> Kafka (V.Vector CommitTopicResult)
offsetCommit = fmap offsetCommitResponseV2Results . internal

offsetFetch :: OffsetFetchRequestV1 -> Kafka (V.Vector TopicOffsetResponse)
offsetFetch = fmap offsetFetchResponseV1Topics . internal

produce :: ProduceRequestV0 -> Kafka (V.Vector PublishResult)
produce = fmap produceResponseV0Results . internal

