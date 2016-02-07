{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Network.Kafka.Protocol where
import           Control.Monad
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import           Data.IORef
import           GHC.TypeLits
import           Network.Kafka.Types
import           Network.Simple.TCP hiding (send)
import qualified Network.Socket.ByteString.Lazy as Lazy
import           Network.Kafka.Exports
import           Network.Kafka.Primitive.ConsumerMetadata
import           Network.Kafka.Primitive.Fetch
import           Network.Kafka.Primitive.Metadata
import           Network.Kafka.Primitive.Offset
import           Network.Kafka.Primitive.OffsetCommit
import           Network.Kafka.Primitive.OffsetFetch
import           Network.Kafka.Primitive.Produce

req :: CorrelationId -> Utf8 -> RequestMessage a v -> Request a v
req = Request

data KafkaConnection = KafkaConnection
  { kafkaSocket    :: Socket
  , kafkaLeftovers :: IORef BS.ByteString
  }

withKafkaConnection :: HostName -> ServiceName -> (KafkaConnection -> IO a) -> IO a
withKafkaConnection h p f = connect h p $ \(s, _) -> do
  ref <- newIORef BS.empty
  f $ KafkaConnection s ref

createKafkaConnection :: HostName -> ServiceName -> IO KafkaConnection
createKafkaConnection h p = do
  ref <- newIORef BS.empty
  (s, _) <- connectSock h p
  return $ KafkaConnection s ref

closeKafkaConnection :: KafkaConnection -> IO ()
closeKafkaConnection c = do
  writeIORef (kafkaLeftovers c) $ error "Connection closed"
  closeSock $ kafkaSocket c

class (KnownNat v,
       ByteSize (RequestMessage p v),
       Binary (RequestMessage p v),
       ByteSize (ResponseMessage p v),
       Binary (ResponseMessage p v),
       RequestApiKey p) => KafkaAction p (v :: Nat)
instance KafkaAction ConsumerMetadata 0
instance KafkaAction Fetch 0
instance KafkaAction Metadata 0
instance KafkaAction Offset 0
instance KafkaAction OffsetCommit 0
instance KafkaAction OffsetCommit 1
instance KafkaAction OffsetCommit 2
-- TODO make sure that v0 reads from ZooKeeper
instance KafkaAction OffsetFetch 0
instance KafkaAction OffsetFetch 1
instance KafkaAction Produce 0

withByteBoundary :: Get a -> Get a
withByteBoundary g = do
  bytesToRead <- fromIntegral <$> getWord32be
  isolate bytesToRead g

-- | TODO, send hangs on produce with acks set to 0 since no response is sent in
-- this case
send :: (KafkaAction p v) => KafkaConnection -> Request p v -> IO (Response p v)
send c req = do
  let putVal = runPut $ do
        put $ byteSize req
        put req
  void $ Lazy.send (kafkaSocket c) putVal
  left <- readIORef $ kafkaLeftovers c
  let d = runGetIncremental (withByteBoundary get)
  go $ if BS.null left then d else pushChunk d left
  where
    go d = case d of
      Fail consumed rem err -> do
        error err -- TODO
      Partial f -> recv (kafkaSocket c) 2048 >>= \x -> (go $ f x)
      Done rest _ x -> writeIORef (kafkaLeftovers c) rest >> return x

sendNoResponse :: (KafkaAction p v) => KafkaConnection -> Request p v -> IO ()
sendNoResponse c req = do
  let putVal = runPut $ do
        put $ byteSize req
        put req
  void $ Lazy.send (kafkaSocket c) putVal

