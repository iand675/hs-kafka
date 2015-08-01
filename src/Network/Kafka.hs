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
import Control.Monad
import Control.Monad.Fix
import Control.Monad.Trans
import Control.Monad.Trans.State.Strict
import Data.Int
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Network.Kafka.Exports (ByteSize, byteSize)
import qualified Network.Kafka.Fields as F
import Network.Kafka.Primitive.ConsumerMetadata
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.Produce
import Network.Kafka.Protocol
import Network.Kafka.Types


data KafkaState = KafkaState
  { kafkaStateSupply   :: !Supply
  , kafkaStateClient   :: !KafkaClient
  , kafkaStateClientId :: !Utf8
  }

newtype KafkaT m a = KafkaT { execKafkaT :: StateT KafkaState m a }
  deriving (Functor, Applicative, Monad, MonadFix, MonadTrans, Alternative, MonadPlus, MonadIO)

data Coordinator = Coordinator
  { coordinatorId   :: CoordinatorId
  , coordinatorHost :: T.Text
  , coordinatorPort :: Int
  } deriving (Show, Eq)

request :: Monad m => RequestMessage p v -> KafkaT m (Request p v)
request r = KafkaT $ do
  st <- get
  let (ident, supply') = freshId $ kafkaStateSupply st
  put $ st { kafkaStateSupply = supply' }
  return $ Request (CorrelationId $ fromIntegral ident) (kafkaStateClientId st) r

newtype ConsumerGroup = ConsumerGroup { fromConsumerGroup :: T.Text }

getConsumerMetadata :: MonadIO m => ConsumerGroup -> KafkaT m (Either ErrorCode Coordinator)
getConsumerMetadata g = KafkaT $ do
  req <- execKafkaT $ request $ ConsumerMetadataRequestV0 $ Utf8 $ T.encodeUtf8 $ fromConsumerGroup g
  client <- kafkaStateClient <$> get
  resp' <- liftIO $ send client req
  let resp = responseMessage resp'
  return $ case F.view F.errorCode resp of
    NoError -> Right $ Coordinator (F.view F.coordinatorId resp) (F.view (F.coordinatorHost . F.to (T.decodeUtf8 . fromUtf8)) resp) (F.view (F.coordinatorPort . F.to fromIntegral) resp)
    err -> Left err

runKafkaT :: MonadIO m => KafkaClient -> T.Text -> KafkaT m a -> m a
runKafkaT client clientId m = do
  s <- liftIO newSupply
  evalStateT (execKafkaT m) (KafkaState s client $ Utf8 $ T.encodeUtf8 clientId) 

-- fetch
-- getMetadata
-- getOffsetRanges
-- commitOffsets
-- getOffsets

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

send :: 
