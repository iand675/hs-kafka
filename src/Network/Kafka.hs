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
import Network.Kafka.Internal.Client
import Network.Kafka.Primitive
import Network.Kafka.Primitive.GroupCoordinator
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.Produce
import Network.Kafka.Protocol
import Network.Kafka.Types
import System.IO.Unsafe

localConfig :: KafkaConfig
localConfig = defaultConfig
  { kafkaConfigBootstrapServers = V.singleton ("localhost", "9092")
  }


data Consumer f k v = Consumer
  { consumerClient :: KafkaClient
  , consumerKeyDeserializer :: BL.ByteString -> f k
  , consumerValueDeserializer :: BL.ByteString -> f v
  }


