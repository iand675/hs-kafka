{-# LANGUAGE OverloadedStrings #-}
module ConnectionTests where
import qualified Data.IntMap.Strict as I
import           Data.IORef
import qualified Data.Vector as V
import           Network.Kafka.Internal.Connection
import           Network.Kafka.Types
import           Network.Kafka.Primitive.Metadata (MetadataRequestV0(..))
import           Test.Tasty
import           Test.Tasty.HUnit

connectionTests :: TestTree
connectionTests = testGroup "Connection tests"
  [ testCase "new kafka connection has no leftovers" $ do
      (t, _, _) <- mockTransport
      conn <- createKafkaConnection' "" "" t
      left <- readIORef $ kafkaLeftovers conn
      assertEqual "kafkaLeftovers" "" left

  , testCase "new kafka connection has no pending response" $ do
      (t, _, _) <- mockTransport
      conn <- createKafkaConnection' "" "" t
      pending <- readIORef $ kafkaPendingResponses conn
      assertBool "kafkaPendingResponses" $ I.null pending

  , testCase "send causes new response promise to be added" $ do
      (t, _, _) <- mockTransport
      conn <- createKafkaConnection' "" "" t
      r <- send conn defaultConfig $ MetadataRequestV0 V.empty
      pending <- readIORef $ kafkaPendingResponses conn
      assertBool "kafkaPendingResponses" $ not $ I.null pending

  , testCase "send creates new correlation id" $ do
      (t, _, _) <- mockTransport
      conn <- createKafkaConnection' "" "" t
      r1 <- send conn defaultConfig $ MetadataRequestV0 V.empty
      r2 <- send conn defaultConfig $ MetadataRequestV0 V.empty
      pending <- readIORef $ kafkaPendingResponses conn
      assertEqual "kafkaPendingResponses" 2 $ length $ I.keys pending

  , testCase "send causes responses to flush if too many are in flight" $ do
      return ()

  , testCase "sendNoResponse doesn't enqueue request to be fulfilled." $ do
      (t, _, _) <- mockTransport
      conn <- createKafkaConnection' "" "" t
      r <- sendNoResponse conn defaultConfig $ MetadataRequestV0 V.empty
      pending <- readIORef $ kafkaPendingResponses conn
      assertBool "kafkaPendingResponses" $ I.null pending

  , testCase "recv can parse all API response types" $ do
      return ()

  , testCase "recv uses unconsumed data as part of next recv call" $ do
      return ()

  , testCase "recv causes responses from previous sends to be fulfilled with correct responses" $ do
      return ()

  , testCase "recv causes fulfilled promises to be removed from connection" $ do
      return ()

  , testCase "reconnect triggers errors on unfulfilled requests" $ do
      return ()

  , testCase "reconnect drops pending requests from tracked queue" $ do
      return ()

  , testCase "reconnect drops leftovers" $ do
      return ()

  , testCase "reconnect prevents connections from sending to bad socket while reconnecting" $ do
      return ()

  , testCase "reconnect throws specialized exception on reconnection failure" $ do
      return ()

  , testCase "reconnect honors configured connection timeout" $ do
      return ()
  ]

