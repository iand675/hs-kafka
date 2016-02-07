module Network.Kafka.Consumer.Config where

data PartitionAssignmentStrategy
  = Range
  | RoundRobin

data AutoOffsetReset
  = Latest
  | Earliest
  | None

data RebalanceCallback = {- forall k v. -} RebalanceCallback
  { rebalanceCallbackOnPartitionsAssigned :: V.Vector TopicPartition {- -> Consumer k v -} -> IO ()
  , rebalanceCallbackOnPartitionsRevoked :: V.Vector TopicPartition {- -> Consumer k v -} -> IO ()
  }

data ConsumerConfig = ConsumerConfig
  { consumerConfigBootstrapServers :: [(String, Port)]
  , consumerConfigGroupId :: Text
  , consumerConfigPartitionAssignmentStrategy :: PartitionAssignmentStrategy
  , consumerConfigMetadataMaxAge :: Int64
  , consumerConfigEnableAutoCommit :: Bool
  , consumerConfigAutoCommitInterval :: Int64
  , consumerConfigClientId :: Text
  , consumerConfigSendBuffer :: Int
  , consumerConfigReceiveBuffer :: Int
  , consumerConfigFetchMinBytes :: Int
  , consumerConfigFetchMaxWaitMs :: Int
  , consumerConfigReconnectBackoffMs :: Int64
  , consumerConfigRetryBackoffMs :: Int64
  , consumerConfigAutoOffsetReset :: AutoOffsetReset
  , consumerConfigRebalanceCallback :: RebalanceCallback
  , consumerConfigCheckCrcs :: Bool
  , consumerConfigConnectionsMaxIdleMs :: Int64
  } deriving (Read, Show)

defaultConsumerConfig = ConsumerConfig
  { consumerConfigBootstrapServers = []
  , consumerConfigGroupId = ""
  , consumerConfigPartitionAssignmentStrategy = Range
  , consumerConfigMetadataMaxAge = 5 * 70 * 1000
  , consumerConfigEnableAutoCommit = True
  , consumerConfigAutoCommitInterval = 5000
  , consumerConfigClientId = ""
  , consumerConfigSendBuffer = 128 * 1024
  , consumerConfigReceiveBuffer = 32 * 1024
  , consumerConfigFetchMinBytes = 1024
  , consumerConfigFetchMaxWaitMs = 500
  , consumerConfigReconnectBackoffMs = 50
  , consumerConfigRetryBackoffMs = 100
  , consumerConfigAutoOffsetReset = Latest
  , consumerConfigRebalanceCallback = RebalanceCallback (const $ return ()) (const $ return ())
  , consumerConfigCheckCrcs = True
  , consumerConfigConnectionsMaxIdleMs = 9 * 60 * 1000
  }
