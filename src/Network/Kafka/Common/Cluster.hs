module Network.Kafka.Common.Cluster where
import Data.HashMap.Strict (HashMap)
import qualified Data.Vector as V

data Cluster = Cluster
  { clusterPartitionsByTopicPartition :: HashMap TopicPartition PartitionInfo
  , clusterPartitionsByTopic          :: HashMap Utf8 (V.Vector PartitionInfo)
  , clusterAvailablePartitionsByTopic :: HashMap Utf8 (V.Vector PartitionInfo)
  , clusterPartitionsByNode           :: HashMap NodeId (V.Vector PartitionInfo)
  , clusterNodesById                  :: HashMap NodeId Node
  }

