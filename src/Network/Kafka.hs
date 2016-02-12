{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE StandaloneDeriving #-}
module Network.Kafka where

import Control.Applicative
import Control.Concurrent.Supply
import Control.Lens
import Control.Monad
import Control.Monad.Fix
import Control.Monad.Trans
import Control.Monad.Trans.State.Strict
import Data.Int
import Data.IORef
import Data.Hashable (Hashable(..))
import Data.Binary (Binary, encode)
import qualified Data.HashMap.Strict as H
import qualified Data.IntMap.Strict as I
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Network.Kafka.Exports (ByteSize, byteSize)
import qualified Network.Kafka.Fields as F
import Network.Kafka.Client
import Network.Kafka.Primitive.GroupCoordinator
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.Produce
import Network.Kafka.Protocol
import Network.Kafka.Simple
import Network.Kafka.Types
import System.Random.MWC (createSystemRandom, uniform)

localConfig :: KafkaConfig
localConfig = defaultConfig
  { kafkaConfigBootstrapServers = V.singleton ("localhost", "9092")
  }

data Producer k v = Producer
  { producerClient :: KafkaClient
  , producerKeySerializer :: k -> BL.ByteString
  , producerValueSerializer :: v -> BL.ByteString
  , producerPartitioner :: Partitioner k
  }

createProducer :: KafkaConfig
               -> (k -> BL.ByteString)
               -> (v -> BL.ByteString)
               -> Partitioner k
               -> IO (Producer k v)
createProducer c kf vf p = do
  client <- kafkaClient c
  return $ Producer client kf vf p 

simpleProducer :: (Binary k, Binary v)
               => KafkaConfig
               -> IO (Producer k v)
simpleProducer c = do
  p <- makeRoundRobinPartitioner
  createProducer c encode encode p

sendValue :: Monoid k => Producer k v -> Utf8 -> v -> IO RecordMetadata
sendValue p t = Network.Kafka.send p t mempty

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
      let m = Message (Attributes NoCompression) (Bytes $ producerKeySerializer p k) (Bytes $ producerValueSerializer p v)
          ms = MessageSet [ MessageSetItem 0 m ]
      result <- runKafka (KafkaContext conn $ kafkaClientConfig client)
                  $ produce $ V.singleton $ TopicPublish t $ V.singleton
                  $ PartitionMessages pid ms
      print result
      return undefined


type Partitioner k = (k -> Int32 -> IO PartitionId)

makeDefaultPartitioner :: Hashable k => IO (Partitioner (Maybe k))
makeDefaultPartitioner = do
  gen <- createSystemRandom
  return $ \mk ps -> case mk of
    Nothing -> do
      i <- uniform gen
      return $ PartitionId (i `mod` ps)
    Just k -> return $ PartitionId (fromIntegral (hash k) `mod` ps)

makeRoundRobinPartitioner :: IO (Partitioner k)
makeRoundRobinPartitioner = do
  nextPartition <- newIORef 0
  return $ \_ ps -> do
    i <- atomicModifyIORef' nextPartition (\i -> (i + 1, i))
    return $ PartitionId (i `mod` ps)

makeRandomizedPartitioner :: IO (Partitioner k)
makeRandomizedPartitioner = do
  gen <- createSystemRandom
  return $ \_ ps -> do
    i <- uniform gen
    return $ PartitionId (i `mod` ps)

hashedPartitioner :: Hashable k => Partitioner k
hashedPartitioner k ps = pure $ PartitionId (fromIntegral (hash k) `mod` ps)

