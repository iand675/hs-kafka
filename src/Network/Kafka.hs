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
import qualified Data.Vector as V
import Network.Kafka.Exports (ByteSize, byteSize)
import Network.Kafka.Primitive.ConsumerMetadata
import Network.Kafka.Primitive.Fetch
import Network.Kafka.Primitive.Metadata
import Network.Kafka.Primitive.Offset
import Network.Kafka.Primitive.OffsetCommit
import Network.Kafka.Primitive.Produce
import Network.Kafka.Protocol
import Network.Kafka.Types

check :: Binary a => a -> (BL.ByteString, Int64, a)
check x = let bs = runPut $ put x in (bs, BL.length bs, runGet get bs)

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

asserting :: (Show a, Eq a, Binary a, ByteSize a) => a -> IO ()
asserting x = do
  putStrLn ("Checking " ++ show x)
  let (bs, c, x') = check x
  if fromIntegral (byteSize x) /= c
    then putStrLn "Invalid byteSize!\n"
    else if x /= x'
      then putStrLn "Bad round-trip!\n"
      else putStrLn "OK!\n"


r :: RequestMessage a v -> Request a v
r = Request (CorrelationId 12) (Utf8 "client")

checkAll = do
  asserting $ Array $ V.fromList [1,2,3,4 :: Int16]
  asserting $ FixedArray $ V.fromList [1,2,3,4 :: Int32]
  asserting $ NoError
  asserting $ ApiKey 12
  asserting $ ApiVersion 3
  asserting $ CorrelationId 342
  asserting $ CoordinatorId 103
  asserting $ NodeId 2130
  asserting $ PartitionId 3024032
  asserting $ ConsumerId $ Utf8 "walrus"
  asserting $ Utf8 "hi there folks!"
  asserting $ Bytes "hey ho weirweirwe"
  asserting $ Attributes GZip
  asserting $ Message 0 (Attributes Snappy) (Bytes "yo") (Bytes "hi")
  asserting $ MessageSetItem 0 $ Message 1 (Attributes NoCompression) (Bytes "foo") (Bytes "bar")
  asserting $ ConsumerMetadataRequestV0 $ Utf8 "consumer-group"
  asserting $ r $ ConsumerMetadataRequestV0 $ Utf8 "consumer-group"
