{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Network.Kafka.Protocol where
import           Control.Monad
import qualified Data.ByteString.Lazy as BL
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import           Data.IORef
import           GHC.TypeLits
import           Network.Kafka.Types
import           Network.Simple.TCP hiding (send)
import qualified Network.Socket.ByteString.Lazy as Lazy
import           Network.Kafka.Primitive.ConsumerMetadata
import           Network.Kafka.Primitive.Fetch
import           Network.Kafka.Primitive.Metadata
import           Network.Kafka.Primitive.Offset
import           Network.Kafka.Primitive.OffsetCommit
import           Network.Kafka.Primitive.OffsetFetch
import           Network.Kafka.Primitive.Produce

req :: CorrelationId -> Utf8 -> RequestMessage a v -> Request a v
req = Request

data KafkaClient = KafkaClient { kafkaSocket    :: Socket
                               , kafkaLeftovers :: IORef BL.ByteString
                               }

withKafkaClient :: HostName -> ServiceName -> (KafkaClient -> IO a) -> IO a
withKafkaClient h p f = connect h p $ \(s, _) -> do
  ref <- newIORef =<< Lazy.getContents s
  f $ KafkaClient s ref

class (RequestApiKey p, Binary (Request p v), Binary (Response p v)) => KafkaAction p (v :: Nat)
instance KafkaAction ConsumerMetadata 0
instance KafkaAction Fetch 0
instance KafkaAction Metadata 0
instance KafkaAction Offset 0
instance KafkaAction OffsetCommit 0
instance KafkaAction OffsetCommit 1
instance KafkaAction OffsetCommit 2
-- TODO make sure that v0 reads from Kafka
instance KafkaAction OffsetFetch 0
instance KafkaAction OffsetFetch 1
instance KafkaAction Produce 0

send :: KafkaAction p v => KafkaClient -> Request p v -> IO (Response p v)
send c req = do
  void $ Lazy.send (kafkaSocket c) $ runPut $ put req
  left <- readIORef $ kafkaLeftovers c
  case runGetOrFail get left of
    Left (_, _, err) -> error err
    Right (rest, _, x) -> writeIORef (kafkaLeftovers c) rest >> return x

