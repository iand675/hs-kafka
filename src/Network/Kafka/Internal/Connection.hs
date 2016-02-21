{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE OverloadedStrings          #-}
module Network.Kafka.Internal.Connection (
  KafkaConnection(..),
  KafkaContext(..),
  nextCorrelationId,
  -- withKafkaConnection,
  createKafkaConnection,
  closeKafkaConnection,
  requestFulfiller,
  streamGet,
  recv,
  send,
  sendNoResponse,
  reconnect,
  KafkaAction(..)
) where
import           Control.Arrow
import           Control.Concurrent.MVar
import           Control.Exception
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans.State hiding (get, put)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import           Data.IORef
import qualified Data.IntMap as I
import           Data.Monoid
import           GHC.TypeLits
import           Network.Kafka.Types
import           Network.Simple.TCP hiding (send, recv)
import qualified Network.Simple.TCP as TCP
import qualified Network.Socket.ByteString.Lazy as Lazy
import           Network.Kafka.Exports
import           Network.Kafka.Primitive.GroupCoordinator
import           Network.Kafka.Primitive.Fetch
import           Network.Kafka.Primitive.Metadata
import           Network.Kafka.Primitive.Offset
import           Network.Kafka.Primitive.OffsetCommit
import           Network.Kafka.Primitive.OffsetFetch
import           Network.Kafka.Primitive.Produce
import           System.IO.Unsafe (unsafeInterleaveIO)

data Transport = Transport
  { transportSend      :: BL.ByteString -> IO ()
  , transportRecv      :: Int -> IO (Maybe BS.ByteString)
  , transportClose     :: IO ()
  }

socketTransport :: HostName -> ServiceName -> IO Transport
socketTransport h p = do
  (s, _) <- connectSock h p
  return $ Transport (void . Lazy.send s) (TCP.recv s) (closeSock s)

mockTransport :: IO (Transport, IORef [BS.ByteString], IORef BL.ByteString)
mockTransport = do
  inBuffer <- newIORef []
  outBuffer <- newIORef BL.empty
  closed <- newIORef False
  -- TODO mockSend should throw if connection closed
  let mockSend cs = atomicModifyIORef' inBuffer (\c -> (c ++ BL.toChunks cs, ()))
      mockRecv i = do
        x <- atomicModifyIORef' outBuffer (tryTakeChunk i)
        isClosed <- readIORef closed
        if isClosed || BS.null x
          then return Nothing
          else return $ Just x
      mockClose = writeIORef closed True
  return (Transport mockSend mockRecv mockClose, inBuffer, outBuffer)
  where
    tryTakeChunk :: Int -> BL.ByteString -> (BL.ByteString, BS.ByteString)
    tryTakeChunk i = (\(bs, rem) -> (rem, BL.toStrict bs)) . BL.splitAt (fromIntegral i)

req :: CorrelationId -> Utf8 -> req -> Request req
req = Request

until' :: IO Bool -> IO () -> IO ()
until' mPred m = do
  pred <- mPred
  unless pred (m >> until' mPred m)

data ResponsePromise
  = Produce (MVar ProduceResponseV0)
  | Fetch (MVar FetchResponseV0)
  | Offset (MVar OffsetResponseV0)
  | Metadata (MVar MetadataResponseV0)
  | OffsetCommit0 (MVar OffsetCommitResponseV0)
  | OffsetCommit1 (MVar OffsetCommitResponseV1)
  | OffsetCommit2 (MVar OffsetCommitResponseV2)
  | OffsetFetch0 (MVar OffsetFetchResponseV0)
  | OffsetFetch1 (MVar OffsetFetchResponseV1)
  | GroupCoordinator (MVar GroupCoordinatorResponseV0)
  | JoinGroup
  | Heartbeat
  | LeaveGroup
  | SyncGroup
  | DescribeGroups
  | ListGroups

makePrisms ''ResponsePromise

-- TODO notes about thread safety
data KafkaConnection = KafkaConnection
  { kafkaConnectionInfo    :: (HostName, ServiceName)
  , kafkaSocket            :: MVar Transport
  , kafkaLeftovers         :: IORef BS.ByteString
  , kafkaPendingResponses  :: IORef (I.IntMap ResponsePromise)
  , kafkaCorrelationSupply :: IORef Int
  -- , kafkaEnqueuedRequests  :: TVar Builder 
  -- , kafkaRequestsInFlight  :: TVar Int
  }

data KafkaContext = KafkaContext
  { kafkaContextConnection :: !KafkaConnection
  , kafkaContextConfig     :: !KafkaConfig
  }

nextCorrelationId :: KafkaConnection -> IO CorrelationId
nextCorrelationId c = atomicModifyIORef'
  (kafkaCorrelationSupply c)
  (\x -> (x + 1, CorrelationId $ fromIntegral x))

{-
withKafkaConnection :: HostName -> ServiceName -> KafkaConfig -> (KafkaConnection -> IO a) -> IO a
withKafkaConnection h p conf f = connect h p $ \(s, _) -> do
  ms <- newMVar s
  leftoverRef <- newIORef BS.empty
  cidRef <- newIORef 0
  pendingRef <- newIORef I.empty
  let conn = KafkaConnection (h, p) ms leftoverRef pendingRef cidRef
  x <- f conn
  until' (I.null <$> readIORef pendingRef) $ recv conn conf
  return x
-}

createKafkaConnection :: HostName -> ServiceName -> IO KafkaConnection
createKafkaConnection h p = do
  leftoverRef <- newIORef BS.empty
  cidRef <- newIORef 0
  pendingRef <- newIORef I.empty
  s <- socketTransport h p
  ms <- newMVar s
  return $ KafkaConnection (h, p) ms leftoverRef pendingRef cidRef

closeKafkaConnection :: KafkaConnection -> KafkaConfig -> IO ()
closeKafkaConnection conn conf = do
  writeIORef (kafkaLeftovers conn) $ error "Connection closed"
  until' (I.null <$> readIORef (kafkaPendingResponses conn)) $ recv conn conf
  transportClose =<< readMVar (kafkaSocket conn)

class ( ByteSize req
      , Binary req
      , ByteSize resp
      , Binary resp
      , RequestApiKey req
      , RequestApiVersion req
      ) => KafkaAction req resp | req -> resp, resp -> req where
  requestFulfiller :: Prism' ResponsePromise (MVar resp)

instance KafkaAction GroupCoordinatorRequestV0 GroupCoordinatorResponseV0 where
  requestFulfiller = _GroupCoordinator

instance KafkaAction FetchRequestV0 FetchResponseV0 where
  requestFulfiller = _Fetch

instance KafkaAction MetadataRequestV0 MetadataResponseV0 where
  requestFulfiller = _Metadata

instance KafkaAction OffsetRequestV0 OffsetResponseV0 where
  requestFulfiller = _Offset

instance KafkaAction OffsetCommitRequestV0 OffsetCommitResponseV0 where
  requestFulfiller = _OffsetCommit0

instance KafkaAction OffsetCommitRequestV1 OffsetCommitResponseV1 where
  requestFulfiller = _OffsetCommit1

instance KafkaAction OffsetCommitRequestV2 OffsetCommitResponseV2 where
  requestFulfiller = _OffsetCommit2

-- TODO make sure that v0 reads from ZooKeeper
instance KafkaAction OffsetFetchRequestV0 OffsetFetchResponseV0 where
  requestFulfiller = _OffsetFetch0

instance KafkaAction OffsetFetchRequestV1 OffsetFetchResponseV1 where
  requestFulfiller = _OffsetFetch1

instance KafkaAction ProduceRequestV0 ProduceResponseV0 where
  requestFulfiller = _Produce

withByteBoundary :: Get a -> Get a
withByteBoundary g = do
  bytesToRead <- fromIntegral <$> getWord32be
  isolate bytesToRead g

streamGet :: ( HasReceiveBufferBytes conf Int )
          => KafkaConnection
          -> conf
          -> Get a
          -> IO a
streamGet c conf g = do
  left <- readIORef $ kafkaLeftovers c
  let d = runGetIncremental g
  withMVar (kafkaSocket c) $ \sock -> go sock (if BS.null left then d else pushChunk d left)
  where
    go sock d = case d of
      Fail consumed rem err -> error err -- TODO
      Partial f -> transportRecv sock (conf ^. receiveBufferBytes) >>= (go sock . f)
      Done rest _ x -> do
        writeIORef (kafkaLeftovers c) rest
        return x


-- TODO handle socket exceptions?
recv :: (HasReceiveBufferBytes conf Int) => KafkaConnection -> conf -> IO ()
recv c conf = do
  (responseLength, correlation@(CorrelationId cid)) <- streamGet c conf ((,) <$> getInt32be <*> get)
  let ix = fromIntegral cid
  mVal <- atomicModifyIORef' (kafkaPendingResponses c) (I.delete ix &&& I.lookup ix)
  case mVal of
    Nothing -> return ()
    Just val -> case val of
      Produce var          -> fillStream responseLength var
      Fetch var            -> fillStream responseLength var
      Offset var           -> fillStream responseLength var
      Metadata var         -> fillStream responseLength var
      OffsetCommit2 var    -> fillStream responseLength var
      OffsetFetch1 var     -> fillStream responseLength var
      GroupCoordinator var -> fillStream responseLength var
      -- Old protocol impls
      OffsetCommit1 var    -> fillStream responseLength var
      OffsetCommit0 var    -> fillStream responseLength var
      OffsetFetch0 var     -> fillStream responseLength var
  where
    fillStream :: Binary resp => Int32 -> MVar resp -> IO ()
    fillStream l m = do
      x <- streamGet c conf $ isolate (fromIntegral (l - byteSize (undefined :: CorrelationId))) get
      putMVar m x

-- | TODO, send hangs on produce with acks set to 0 since no response is sent in
-- this case
send :: forall req resp conf.
  ( KafkaAction req resp
  , HasClientId conf Utf8
  , HasReceiveBufferBytes conf Int
  ) => KafkaConnection -> conf -> req -> IO resp
send c conf req = do
  correlation@(CorrelationId cid) <- sendNoResponse c conf req
  promise <- newEmptyMVar
  atomicModifyIORef' (kafkaPendingResponses c)
                     (\m -> (I.insert (fromIntegral cid)
                                      (requestFulfiller # (promise :: MVar resp)) m, ()))
  {-
  mvar <- newEmptyMVar
  atomically $ do
    cId <- readTVar $ kafkaCorrelationSupply c
    modifyTVar' (kafkaPendingResponses c) (IM.insert i _) mvar
    modifyTVar' (kafkaEnqueuedRequests c) (<> putVal)
    modifyTVar' (kafkaRequestsInFlight c) (+ 1)
    writeTVar (kafkaCorrelationSupply c) (+ 1)
  -}
  unsafeInterleaveIO $ do
    until' (not <$> isEmptyMVar promise) $ recv c conf
    takeMVar promise


sendNoResponse :: ( KafkaAction req resp
                  , HasClientId conf Utf8
                  ) => KafkaConnection -> conf -> req -> IO CorrelationId
sendNoResponse c conf req = do
  correlation <- nextCorrelationId c
  let putVal = runPut $ do
        let r = Request correlation (conf ^. clientId) req
        put $ byteSize r
        put r
  withMVar (kafkaSocket c) $ \sock -> transportSend sock putVal
  return correlation

reconnect :: KafkaConnection -> IO ()
reconnect c = do
  sock <- takeMVar $ kafkaSocket c
  transportClose sock
  let (h, p) = kafkaConnectionInfo c
  sock' <- socketTransport h p
  atomicModifyIORef (kafkaLeftovers c) $ const ("", ())
  putMVar (kafkaSocket c) sock'



