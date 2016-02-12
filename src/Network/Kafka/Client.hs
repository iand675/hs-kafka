{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE TypeSynonymInstances   #-}
{-# LANGUAGE FlexibleInstances      #-}
module Network.Kafka.Client where

import Control.Arrow
import Control.Exception
import Control.Lens
import Data.IORef
import qualified Data.IntMap as I
import qualified Data.HashMap.Strict as H
import qualified Data.HashSet as S
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import qualified Data.Vector as V
import qualified Network.Kafka.Fields as K
import Network.Kafka.Protocol
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Types
import Network.Kafka.Simple
import Network.Simple.TCP

type TrackedTopic = ()

data KafkaClientState = KafkaClientState
  { kafkaClientStateBootstrapServers :: V.Vector (HostName, ServiceName)
  , kafkaClientStateBootstrappedConn :: (HostName, ServiceName, KafkaConnection)
  , kafkaClientStateConnections      :: I.IntMap KafkaConnection
  , kafkaClientStateTrackedTopics    :: S.HashSet Utf8
  , kafkaClientStateNodesById        :: I.IntMap Broker
  , kafkaClientStateTopicMetadata    :: H.HashMap Utf8 TopicMetadata
  }

makeFields ''KafkaClientState

data KafkaClient = KafkaClient 
  { kafkaClientState  :: IORef KafkaClientState
  , kafkaClientConfig :: KafkaConfig
  }

kafkaClient :: KafkaConfig
            -- ^ Bootstrap servers
            -> IO KafkaClient
kafkaClient conf = do
  mBsConn <- acquireInitialConnection (conf ^. bootstrapServers)
  case mBsConn of
    Nothing -> error "Unable to connect to Kafka"
    Just bsConn -> do
      let st = KafkaClientState (conf ^. bootstrapServers) bsConn I.empty S.empty I.empty H.empty
      c <- KafkaClient <$> newIORef st <*> pure conf
      return c

acquireInitialConnection 
  :: V.Vector (HostName, ServiceName)
  -> IO (Maybe (HostName, ServiceName, KafkaConnection))
acquireInitialConnection vec = go 0 
  where
    go i = if i < V.length vec
           then do
             let (h, s) = vec V.! i
             mConn <- try $ createKafkaConnection h s
             case mConn of
               Left (err :: IOException) -> do
                 print err
                 go (i + 1)
               Right conn -> return $ Just (h, s, conn)
           else return Nothing

refreshMetadata :: KafkaClient -> IO ()
refreshMetadata client = do
  st <- readClientState client
  let ctxt = KafkaContext (st ^. bootstrappedConn . _3) (kafkaClientConfig client)
  meta <- runKafka ctxt $ metadata $ V.fromList $ S.toList $ view trackedTopics st
  let brokerMap = I.fromList $ V.toList $ V.map (fromIntegral . brokerNodeId &&& id) $ meta ^. K.brokers
  let topicMap = H.fromList $ V.toList $ V.map (topicMetadataTopic &&& id) $ meta ^. K.topics
  atomicModifyIORef' (kafkaClientState client) $ \st -> (st & nodesById .~ brokerMap
                                                            & topicMetadata .~ topicMap, ())
  return ()

trackTopic :: Text -> KafkaClient -> IO ()
trackTopic topic client = do
  atomicModifyIORef' (kafkaClientState client) go
  refreshMetadata client
  where
    go st = (st & trackedTopics %~ S.insert (Utf8 $ encodeUtf8 topic), ())

untrackTopic :: Text -> KafkaClient -> IO ()
untrackTopic topic client = do
  atomicModifyIORef' (kafkaClientState client) go
  refreshMetadata client
  where
    go st = (st & trackedTopics %~ S.delete (Utf8 $ encodeUtf8 topic), ())
 
readClientState :: KafkaClient -> IO KafkaClientState
readClientState = readIORef . kafkaClientState

close :: KafkaClient -> IO ()
close client = do
  st <- atomicModifyIORef (kafkaClientState client) $ \st -> (error "KafkaClient closed", st)
  mapM_ (\c -> closeKafkaConnection c (kafkaClientConfig client)) (st ^. connections)
  closeKafkaConnection (st ^. bootstrappedConn . _3) (kafkaClientConfig client)

connectionForNode :: KafkaClient -> NodeId -> IO KafkaConnection
connectionForNode c n = do
  st <- readClientState c
  let i = fromIntegral n
  case st ^. connections . at i of
    Nothing -> do
      case st ^. nodesById . at i of
        -- TODO do node ids change?
        Nothing -> error "Unrecognized node"
        Just b -> do
          conn <- createKafkaConnection (T.unpack $ decodeUtf8 $ fromUtf8 $ b ^. K.host) (show $ b ^. K.port)
          atomicModifyIORef (kafkaClientState c) $ \st' -> (st' & connections %~ I.insert i conn, ())
          return conn
    Just c -> do
      -- TODO, check if socket closed?
      return c
