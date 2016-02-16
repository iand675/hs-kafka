module Network.Kafka.Partitioner where

import Data.Hashable (Hashable(..))
import Data.IORef
import Data.Int
import Network.Kafka.Types
import System.Random.MWC (createSystemRandom, uniform)

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

-- ^ Range partitioning works on a per-topic basis. For each topic, we lay out the available partitions in numeric order and the consumer threads in lexicographic order. We then divide the number of partitions by the total number of consumer streams (threads) to determine the number of partitions to assign to each consumer. If it does not evenly divide, then the first few consumers will have one extra partition.
