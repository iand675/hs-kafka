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
import Network.Kafka.Primitive.GroupCoordinator
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.Produce
import Network.Kafka.Protocol
import Network.Kafka.Types

