module Network.Kafka.Producer
  ( Producer
  , createProducer
  , send
  , sendValue
  , sendTo
  ) where

import           Control.Lens
import           Data.Binary
import qualified Data.ByteString.Lazy as L
import qualified Data.HashMap.Strict as H
import           Data.IORef
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import           Network.Kafka.Protocol
import           Network.Kafka.Primitive
import           Network.Kafka.Primitive.Produce
import qualified Network.Kafka.Fields as F
import           Network.Kafka.Internal.Client
import           Network.Kafka.Internal.Connection (KafkaContext(..))
import           Network.Kafka.Partitioner
import           Network.Kafka.Types
import           System.IO.Unsafe

data Producer k v = Producer
  { producerClient :: KafkaClient
  , producerKeySerializer :: k -> L.ByteString
  , producerValueSerializer :: v -> L.ByteString
  , producerPartitioner :: Partitioner k
  -- TODO replace list with persistent vector
  , producerPending :: IORef (H.HashMap Utf8 [Message])
  }

createProducer :: KafkaConfig
               -> (k -> L.ByteString)
               -> (v -> L.ByteString)
               -> Partitioner k
               -> IO (Producer k v)
createProducer c kf vf p = do
  client <- kafkaClient c
  pending <- newIORef H.empty
  return $ Producer client kf vf p pending

simpleProducer :: (Binary k, Binary v)
               => KafkaConfig
               -> IO (Producer k v)
simpleProducer c = do
  p <- makeRoundRobinPartitioner
  createProducer c encode encode p

sendValue :: Monoid k => Producer k v -> Utf8 -> v -> IO RecordMetadata
sendValue p t = send p t mempty

send :: Producer k v -> Utf8 -> k -> v -> IO RecordMetadata
send p t k v = do
  -- TODO, what do you pick for a partition number if the topic doesn't exist???
  st <- readClientState $ producerClient p
  let partitionCount = maybe 1 (fromIntegral . V.length) (st ^? topicMetadata . at t . _Just . F.partitionMetadata)
  pid <- (producerPartitioner p) k partitionCount
  sendTo p t pid k v

sendTo :: Producer k v -> Utf8 -> PartitionId -> k -> v -> IO RecordMetadata
sendTo p t pid k v = do
  st <- readClientState client
  case H.lookup t (st ^. topicMetadata) of
    -- Since we don't know about the topic yet, register it as a tracked topic
    -- and recurse.
    Nothing -> do
      -- TODO set up a circuit breaker here to be on the safe side
      trackTopic (T.decodeUtf8 $ fromUtf8 t) client
      sendTo p t pid k v
    Just meta -> do
      -- TODO replace this with V.!?
      let mReplica = V.find (\t -> pid == t ^. F.partition) (meta ^. F.partitionMetadata)
      case mReplica of
        Nothing -> go $ st ^. bootstrappedConn . _3 -- TODO
        Just replica -> do
          conn <- connectionForNode client (replica ^. F.leader)
          go conn
  where
    client = producerClient p
    go conn = do
      -- TODO, support batch message sending
      let m = uncompressed (producerKeySerializer p k) (producerValueSerializer p v)
          ms = [ m ]
      -- TODO, another unsafeInterleaveIO here to return the 
      r <- produce (KafkaContext conn $ kafkaClientConfig client)
                   $ V.singleton $ TopicPublish t $ V.singleton $ PartitionMessages pid ms
      unsafeInterleaveIO $ do
        -- TODO replace with implementation that supports batched sending
        let mc = return $ maybe 0 id $ r ^? traverse . F.partitionResults . traverse . F.offset
        commitId <- mc
        return $ RecordMetadata commitId pid t

