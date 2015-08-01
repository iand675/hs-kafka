{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE RankNTypes             #-}
module Network.Kafka.Fields where
import Control.Applicative
import Data.Functor.Contravariant
import Data.Profunctor

type Getter s a = forall f. (Contravariant f, Functor f) => (a -> f a) -> s -> f s
type Lens s t a b = forall f. Functor f => (a -> f b) -> s -> f t
type Lens' s a = Lens s s a a

lens :: (s -> a) -> (s -> b -> t) -> Lens s t a b
lens sa sbt afb s = sbt s <$> afb (sa s)
{-# INLINE lens #-}

-- Internal id functor.
newtype Id a = Id { runId :: a }

-- | Could use @DeriveFunctor@ here but that's not portable.
instance Functor Id where fmap f = Id . f . runId

-- | Get the @a@ inside the @s@.
view :: Getter s a -> s -> a
view l = getConst . l Const

-- | Modify the @a@ inside the @s@, optionally changing the types to
-- @b@ and @t@.
over :: Lens s t a b -> (a -> b) -> s -> t
over l f = runId . l (Id . f)

-- | Set the @a@ inside the @s@, optionally changing the types to @b@
-- and @t@.
set :: Lens s t a b -> b -> s -> t
set l a = runId . l (Id . const a)

to :: (Profunctor p, Contravariant f) => (s -> a) -> p a (f a) -> p s (f s)
to k = dimap k (contramap k)
{-# INLINE to #-}

class HasReplicaId s a | s -> a where
  replicaId :: Lens' s a

class HasMaxWaitTime s a | s -> a where
  maxWaitTime :: Lens' s a

class HasMinBytes s a | s -> a where
  minBytes :: Lens' s a

class HasPartition s a | s -> a where
  partition :: Lens' s a

class HasFetchOffset s a | s -> a where
  fetchOffset :: Lens' s a

class HasMaxBytes s a | s -> a where
  maxBytes :: Lens' s a

class HasData s a | s -> a where
  _data :: Lens' s a

class HasErrorCode s a | s -> a where
  errorCode :: Lens' s a

class HasHighwaterMarkOffset s a | s -> a where
  highwaterMarkOffset :: Lens' s a

class HasMessageSet s a | s -> a where
  messageSet :: Lens' s a

class HasTopics s a | s -> a where
  topics :: Lens' s a

class HasBrokers s a | s -> a where
  brokers :: Lens' s a

class HasNodeId s a | s -> a where
  nodeId :: Lens' s a

class HasHost s a | s -> a where
  host :: Lens' s a

class HasPort s a | s -> a where
  port :: Lens' s a

class HasPartitionMetadata s a | s -> a where
  partitionMetadata :: Lens' s a

class HasLeader s a | s -> a where
  leader :: Lens' s a

class HasReplicas s a | s -> a where
  replicas :: Lens' s a

class HasIsReplicated s a | s -> a where
  isReplicated :: Lens' s a

class HasTopic s a | s -> a where
  topic :: Lens' s a

class HasOffsets s a | s -> a where
  offsets :: Lens' s a

class HasTime s a | s -> a where
  time :: Lens' s a

class HasMaxNumberOfOffsets s a | s -> a where
  maxNumberOfOffsets :: Lens' s a

class HasOffset s a | s -> a where 
  offset :: Lens' s a

class HasConsumerGroup s a | s -> a where
  consumerGroup :: Lens' s a

class HasCoordinatorId s a | s -> a where
  coordinatorId :: Lens' s a

class HasCoordinatorHost s a | s -> a where
  coordinatorHost :: Lens' s a

class HasCoordinatorPort s a | s -> a where
  coordinatorPort :: Lens' s a

class HasPartitions s a | s -> a where
  partitions :: Lens' s a

class HasMessage s a | s -> a where
  message :: Lens' s a

class HasAttributes s a | s -> a where
  attributes :: Lens' s a

class HasKey s a | s -> a where
  key :: Lens' s a

class HasValue s a | s -> a where
  value :: Lens' s a

class HasMagicByte s a | s -> a where
  magicByte :: Lens' s a

class HasCommits s a | s -> a where
  commits :: Lens' s a

class HasMetadata s a | s -> a where
  metadata :: Lens' s a 

class HasResults s a | s -> a where
  results :: Lens' s a

class HasTimestamp s a | s -> a where
  timestamp :: Lens' s a

class HasGeneration s a | s -> a where
  generation :: Lens' s a

class HasConsumer s a | s -> a where
  consumer :: Lens' s a

class HasRetentionTime s a | s -> a where
  retentionTime :: Lens' s a

class HasRequiredAcks s a | s -> a where
  requiredAcks :: Lens' s a

class HasTimeout s a | s -> a where
  timeout :: Lens' s a

class HasTopicPublishes s a | s -> a where
  topicPublishes :: Lens' s a

class HasPartitionResults s a | s -> a where
  partitionResults :: Lens' s a
