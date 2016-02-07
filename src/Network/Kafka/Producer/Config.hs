module Network.Kafka.Producer.Config where
import Data.Text
import Data.Int
import Network.Kafka.Types

data ProducerConfig = ProducerConfig
  { producerConfigBootstrapServers                 :: [(String, Int)]
  , producerConfigBufferMemory                     :: Int64
  , producerConfigRetries                          :: Int
  , producerConfigAcks                             :: AckSetting
  , producerConfigCompressionType                  :: CompressionCodec
  , producerConfigBatchSize                        :: Int
  , producerConfigTimeout                          :: Int
  , producerConfigLingerMs                         :: Int64
  , producerConfigClientId                         :: Text
  , producerConfigSendBuffer                       :: Int
  , producerConfigReceiveBuffer                    :: Int
  , producerConfigMaxRequestSize                   :: Int
  , producerConfigBlockOnBufferFull                :: Bool
  , producerConfigReconnectBackoff                 :: Int64
  , producerConfigRetryBackoff                     :: Int64
  , producerConfigMetadataFetchTimeout             :: Int64
  , producerConfigMetadataMaxAge                   :: Int64
  , producerConfigMaxInFlightRequestsPerConnection :: Int
  , producerConfigConnectionsIdleSecondsConfig     :: Int
  , producerConfigPartitionStrategy                :: PartitionAssignmentStrategy
  }

defaultProducerConfig = ProducerConfig
  { producerConfigBootstrapServers                 = []
  , producerConfigBufferMemory                     = 33554432
  , producerConfigRetries                          = 0
  , producerConfigAcks                             = NeverWait
  , producerConfigCompressionType                  = NoCompression
  , producerConfigBatchSize                        = 16384
  , producerConfigTimeout                          = 30000
  , producerConfigLingerMs                         = 0
  , producerConfigClientId                         = ""
  , producerConfigSendBuffer                       = 131072
  , producerConfigReceiveBuffer                    = 32768
  , producerConfigMaxRequestSize                   = 1048576
  , producerConfigBlockOnBufferFull                = True
  , producerConfigReconnectBackoff                 = 10
  , producerConfigRetryBackoff                     = 100
  , producerConfigMetadataFetchTimeout             = 60000
  , producerConfigMetadataMaxAge                   = 300000
  , producerConfigMaxInFlightRequestsPerConnection = 5
  , producerConfigConnectionsIdleSecondsConfig     = 540
  , producerConfigPartitionStrategy                = DefaultPartitioningStrategy
  }

data AckSetting
  = NeverWait
  -- ^ The producer never waits for an acknowledgement from the broker (the same behavior as 0.7).
  -- This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
  | LeaderAcknowledged
  -- ^ The producer gets an acknowledgement after the leader replica has received the data.
  -- This option provides better durability as the client waits until the server acknowledges
  -- the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
  | AtLeast Int
  -- ^ At least N brokers must have committed the data to their log and acknowledged this to the leader.
  | AllAcknowledged
  -- ^ The producer gets an acknowledgement after all in-sync replicas have received the data.
  -- This option provides the greatest level of durability. However, it does not completely
  -- eliminate the risk of message loss because the number of in sync replicas may, in rare cases, shrink to 1.
  -- If you want to ensure that some minimum number of replicas (typically a majority) receive a write,
  -- then you must set the topic-level min.insync.replicas setting.
  -- Please read the Replication section of the design documentation for a more in-depth discussion.
  deriving (Read, Show, Eq)

data PartitionAssignmentStrategy
  = Range
  -- ^ Range partitioning works on a per-topic basis. For each topic, we lay out the available partitions in numeric order and the consumer threads in lexicographic order. We then divide the number of partitions by the total number of consumer streams (threads) to determine the number of partitions to assign to each consumer. If it does not evenly divide, then the first few consumers will have one extra partition.
  | RoundRobin
  -- ^ The round-robin partition assigner lays out all the available partitions and all the available consumer threads.
  -- It then proceeds to do a round-robin assignment from partition to consumer thread. If the subscriptions of all consumer
  -- instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
  -- will be within a delta of exactly one across all consumer threads.) Round-robin assignment is permitted only if:
  -- (a) Every topic has the same number of streams within a consumer instance (b) The set of subscribed topics is identical
  -- for every consumer instance within the group.
  | DefaultPartitioningStrategy
  -- ^ If a partition is specified in the record, use it. If no partition is specified but a key is present choose a partition based on a hash of the key. If no partition or key is present choose a partition in a round-robin fashion
  deriving (Read, Show, Eq)

