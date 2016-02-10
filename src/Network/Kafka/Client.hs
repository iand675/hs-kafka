module Network.Kafka.Client where

import qualified Data.HashMap.Strict as H
import qualified Data.Vector as V
import Network.Kafka.Protocol
import Network.Kafka.Types
import Network.Simple.TCP

type TrackedTopic = ()
data TrackedNode = TrackedNode
  { trackedNodeHost   :: HostName
  , trackedNodePort   :: ServiceName
  , trackedNodeNodeId :: NodeId
  }

data TopicMetadata = TopicMetadata
  { topicMetadataLeader     :: NodeId
  , topicMetadataReplicas   :: V.Vector NodeId
  , topicMetadataPartitions :: V.Vector PartitionId
  }

data KafkaClient = KafkaClient
  { kafkaClientConnections :: I.IntMap KafkaConnection
  , kafkaTrackedTopics     :: HashSet Utf8
  , kafkaNodesById         :: I.IntMap TrackedNode
  , kafkaTopicMetadata     :: H.HashMap Utf8 TopicMetadata
  }

kafkaClient :: V.Vector (HostName, ServiceName)
            -- ^ Bootstrap servers
            -> IO KafkaClient
kafkaClient bootstraps = do
  conns <- mapM (uncurry createKafkaConnection) bootstraps
  let conns = H.fromList $ V.toList $ V.zipWith bootstraps conns
  return $ KafkaClient conns
