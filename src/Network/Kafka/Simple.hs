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
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Types

newtype Kafka a = Kafka (StateT KafkaConfig IO a)
  deriving (Functor, Applicative, Monad)

data KafkaConfig = KafkaConfig
  { kafkaConfigConnection       :: KafkaConnection
  , kafkaConfigFetchMaxWaitTime :: !Int32
  , kafkaConfigFetchMinBytes    :: !Int32
  , kafkaConfigFetchMaxBytes    :: !Int32
  }

defaultConfig :: KafkaConnection -> KafkaConfig
defaultConfig c = KafkaConfig
  { kafkaConfigConnection = c
  , kafkaConfigFetchMaxWaitTime = 100
  , kafkaConfigFetchMinBytes    = 0
  , kafkaConfigFetchMaxBytes    = 1048576
  }

localKafka :: Kafka a -> IO a
localKafka k = withKafkaConnection "localhost" "9092" $ \c ->
  runKafka (defaultConfig c) k

runKafka :: KafkaConfig -> Kafka a -> IO a
runKafka c (Kafka m) = evalStateT m c

internal :: KafkaAction a v => RequestMessage a v -> Kafka (ResponseMessage a v)
internal r = Kafka $ do
  c <- get
  fmap responseMessage $ liftIO $ send (kafkaConfigConnection c) $ req (CorrelationId 0) (Utf8 "client") r

fetch :: V.Vector TopicFetch -> Kafka (ResponseMessage Fetch 0)
fetch fs = do
  c <- Kafka get
  internal (FetchRequestV0 (NodeId (-1))
                           (kafkaConfigFetchMaxWaitTime c)
                           (kafkaConfigFetchMinBytes c)
                           fs)

metadata :: V.Vector T.Text -> Kafka (ResponseMessage Metadata 0)
metadata = internal . MetadataRequestV0 . V.map (Utf8 . T.encodeUtf8)

offset :: RequestMessage Offset 0 -> Kafka (ResponseMessage Offset 0)
offset = internal

offsetCommit :: RequestMessage OffsetCommit 2 -> Kafka (ResponseMessage OffsetCommit 2)
offsetCommit = internal

offsetFetch :: RequestMessage OffsetFetch 1 -> Kafka (ResponseMessage OffsetFetch 1)
offsetFetch = internal

produce :: RequestMessage Produce 0 -> Kafka (ResponseMessage Produce 0)
produce = internal

