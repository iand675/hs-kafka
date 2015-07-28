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

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Int
import qualified Data.ByteString.Lazy as BL
import Network.Kafka.Exports (byteSize)
import Network.Kafka.Primitive.ConsumerMetadata
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.Produce
import Network.Kafka.Protocol
import Network.Kafka.Types

check :: Binary a => a -> (BL.ByteString, Int64, a)
check x = let bs = runPut $ put x in (BL.drop 4 bs, BL.length bs - 4, runGet get bs)

{-
go $ runGetIncremental get
where
  go (Partial cb) = do
    old <- atomicModifyIORef' (kafkaLeftovers c) $ \old -> (Nothing, old)
    case old of
      Nothing -> do
        chunk <- recv (kafkaSocket c) 4096
        print chunk
        go $ cb chunk
      c -> do
        print c
        go $ cb c
  go (Done left off resp) = do
    modifyIORef' (kafkaLeftovers c) (\leftOld -> leftOld <> if BS.null left then Nothing else Just left)
    putStrLn "done"
    return resp
  go (Fail bs off err) = do
    putStrLn "error"
    error err
-}


