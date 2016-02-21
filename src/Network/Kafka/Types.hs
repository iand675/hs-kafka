{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# LANGUAGE OverloadedStrings          #-}
module Network.Kafka.Types where
import qualified Codec.Compression.GZip as GZip
import           Control.Applicative
import           Control.Lens
import           Control.Monad
import           Debug.Trace
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as L
import           Data.Digest.CRC32
import           Data.Hashable
import           Data.Int
import qualified Data.IntMap.Strict as I
import           Data.IORef
import qualified Data.Vector as V
import qualified Data.Vector.Generic as G
import           GHC.Generics hiding (to)
import           GHC.TypeLits
import           Network.Simple.TCP (HostName, ServiceName, Socket)
import           Network.Kafka.Exports

newtype CorrelationId = CorrelationId { fromCorrelationId :: Int32 }
  deriving (Show, Eq, Ord, Binary, ByteSize)

newtype Utf8 = Utf8 { fromUtf8 :: ByteString }
  deriving (Show, Eq, Hashable)

instance Binary Utf8 where
  get = do
    len <- fromIntegral <$> getInt16be
    Utf8 <$> getByteString len
  put (Utf8 str) = if BS.null str
    then putInt16be (-1)
    else do
      putInt16be $ fromIntegral $ BS.length str
      putByteString str

instance ByteSize Utf8 where
  byteSize (Utf8 bs) = 2 + fromIntegral (BS.length bs)

data Acks
  = All
  | LeaderOnly
  | NoAck

instance Enum Acks where
  toEnum (-1) = All
  toEnum 0 = NoAck
  toEnum 1 = LeaderOnly

  fromEnum All = -1
  fromEnum NoAck = 0
  fromEnum LeaderOnly = 1

data ResetPolicy
  = Earliest
  | Latest
  | None
  | Anything

data KafkaConfig = KafkaConfig
  { kafkaConfigFetchMaxWaitTime                 :: !Int
  , kafkaConfigFetchMinBytes                    :: !Int
  , kafkaConfigFetchMaxBytes                    :: !Int
  , kafkaConfigBootstrapServers                 :: !(V.Vector (HostName, ServiceName))
  , kafkaConfigAcks                             :: !Acks
  , kafkaConfigBufferMemory                     :: !Int
  , kafkaConfigRetries                          :: !Int
  , kafkaConfigBatchSize                        :: !Int
  , kafkaConfigClientId                         :: !Utf8
  , kafkaConfigGroupId                          :: !Utf8
  , kafkaConfigClientMaxIdleMs                  :: !Int
  , kafkaConfigLingerMs                         :: !Int
  , kafkaConfigMaxRequestSize                   :: !Int
  , kafkaConfigReceiveBufferBytes               :: !Int
  , kafkaConfigSendBufferBytes                  :: !Int
  , kafkaConfigBlockOnBufferFull                :: !Bool
  , kafkaConfigMaxInFlightRequestsPerConnection :: !Int
  , kafkaConfigMetadataFetchTimeout             :: !Int
  , kafkaConfigMetadataMaxAgeMs                 :: !Int
  , kafkaConfigReconnectBackoffMs               :: !Int
  , kafkaConfigRetryBackoffMs                   :: !Int
  , kafkaConfigHeartbeatIntervalMs              :: !Int
  , kafkaConfigMaxPartitionFetchBytes           :: !Int
  , kafkaConfigSessionTimeout                   :: !Int
  , kafkaConfigAutoOffsetReset                  :: !ResetPolicy
  , kafkaConfigConnectionMaxIdleMs              :: !Int
  , kafkaConfigEnableAutoCommit                 :: !Bool
  , kafkaConfigAutoCommitInterval               :: !Int
  , kafkaConfigCheckCrcs                        :: !Bool
  , kafkaConfigTimeoutMs                        :: !Int
  }

makeFields ''KafkaConfig

defaultConfig :: KafkaConfig
defaultConfig = KafkaConfig
  { kafkaConfigFetchMaxWaitTime                 = 500
  , kafkaConfigFetchMinBytes                    = 1024
  , kafkaConfigFetchMaxBytes                    = 1024 * 1024
  , kafkaConfigBootstrapServers                 = V.empty
  , kafkaConfigAcks                             = LeaderOnly
  , kafkaConfigBufferMemory                     = 33554432
  , kafkaConfigRetries                          = 0
  , kafkaConfigBatchSize                        = 16384
  , kafkaConfigClientId                         = Utf8 "hs-kafka"
  , kafkaConfigGroupId                          = Utf8 ""
  , kafkaConfigClientMaxIdleMs                  = 540000
  , kafkaConfigLingerMs                         = 0
  , kafkaConfigMaxRequestSize                   = 1048576
  , kafkaConfigReceiveBufferBytes               = 32768
  , kafkaConfigSendBufferBytes                  = 131072
  , kafkaConfigBlockOnBufferFull                = False
  , kafkaConfigMaxInFlightRequestsPerConnection = 5
  , kafkaConfigMetadataFetchTimeout             = 60000
  , kafkaConfigMetadataMaxAgeMs                 = 300000
  , kafkaConfigReconnectBackoffMs               = 50
  , kafkaConfigRetryBackoffMs                   = 100
  , kafkaConfigHeartbeatIntervalMs              = 3000
  , kafkaConfigMaxPartitionFetchBytes           = 1048576
  , kafkaConfigSessionTimeout                   = 30000
  , kafkaConfigAutoOffsetReset                  = Latest
  , kafkaConfigConnectionMaxIdleMs              = 540000
  , kafkaConfigEnableAutoCommit                 = True
  , kafkaConfigAutoCommitInterval               = 5000
  , kafkaConfigCheckCrcs                        = True
  , kafkaConfigTimeoutMs                        = 30000 
  }

putL :: Binary a => Getter s a -> s -> Put
putL l = put . view l

data JoinGroup
data Heartbeat
data LeaveGroup
data SyncGroup
data DescribeGroups
data ListGroups

newtype ApiKey = ApiKey { fromApiKey :: Int16 }
  deriving (Show, Eq, Ord, Binary, ByteSize)

theApiKey :: Int16 -> a -> ApiKey
theApiKey x = const $ ApiKey x

class RequestApiKey req where
  apiKey :: Request req -> ApiKey

instance RequestApiKey JoinGroup where
  apiKey = theApiKey 11

instance RequestApiKey Heartbeat where
  apiKey = theApiKey 12

instance RequestApiKey LeaveGroup where
  apiKey = theApiKey 13

instance RequestApiKey SyncGroup where
  apiKey = theApiKey 14

instance RequestApiKey DescribeGroups where
  apiKey = theApiKey 15

instance RequestApiKey ListGroups where
  apiKey = theApiKey 16

newtype Bytes = Bytes { fromBytes :: L.ByteString }
  deriving (Show, Eq, Hashable)

instance Binary Bytes where
  get = do
    len <- fromIntegral <$> getInt32be
    if len == -1
      then pure $ Bytes L.empty
      else Bytes <$> getLazyByteString len
  put (Bytes bs) = if L.null bs
    then putInt32be (-1)
    else do
      putInt32be $ fromIntegral $ L.length bs
      putLazyByteString bs

instance ByteSize Bytes where
  byteSize (Bytes bs) = 4 + fromIntegral (L.length bs)

data Request req = Request
  { requestCorrelationId :: !CorrelationId
  , requestClientId      :: !Utf8
  , requestMessage       :: !req
  } deriving (Show, Eq, Generic)

makeFields ''Request

instance (RequestApiVersion req,
          RequestApiKey req,
          ByteSize req,
          Binary req) => Binary (Request req) where
  get = do
    _ <- get :: Get ApiKey
    _ <- get :: Get ApiVersion
    Request <$> get <*> get <*> get
  put p = do
    put $ apiKey p
    put $ apiVersion p
    put $ requestCorrelationId p
    put $ requestClientId p
    put $ requestMessage p

class RequestApiVersion req where
  apiVersion :: req -> ApiVersion

instance RequestApiVersion req => RequestApiVersion (Request req) where
  apiVersion r = apiVersion $ requestMessage r

newtype Array v a = Array { fromArray :: v a }
  deriving (Show, Eq, Generic)

instance (Binary a, G.Vector v a) => Binary (Array v a) where
  get = do
    len <- fromIntegral <$> getInt32be
    Array <$> G.replicateM len get
  put (Array v) = do
    putInt32be $ fromIntegral $ G.length v
    G.mapM_ put v

instance (ByteSize a, Functor f, Foldable f) => ByteSize (Array f a) where
  byteSize f = 4 + sum (byteSize <$> fromArray f)

instance ByteSize a => ByteSize (V.Vector a) where
  byteSize v = 4 + G.sum (G.map byteSize v)

newtype FixedArray v a = FixedArray { fromFixedArray :: v a }
  deriving (Show, Eq, Generic)

instance (Binary a, G.Vector v a) => Binary (FixedArray v a) where
  get = (FixedArray . fromArray) <$> get
  put = put . Array . fromFixedArray
  
instance (ByteSize a, G.Vector v a) => ByteSize (FixedArray v a) where
  byteSize (FixedArray v) = 4 + (fromIntegral (G.length v) * singleSize v undefined)
    where
      singleSize :: ByteSize b => f b -> b -> Int32
      singleSize _ = byteSize


data ErrorCode
  = NoError
  | Unknown
  | OffsetOutOfRange
  | CorruptMessage
  | UnknownTopicOrPartition
  | InvalidMessageSize
  | LeaderNotAvailable
  | NotLeaderForPartition
  | RequestTimedOut
  | BrokerNotAvailable
  | ReplicaNotAvailable
  | MessageSizeTooLarge
  | StaleControllerEpoch
  | OffsetMetadataTooLarge
  | GroupLoadInProgress
  | GroupCoordinatorNotAvailable
  | NotCoordinatorForGroup
  | InvalidTopic
  | RecordListTooLarge
  | NotEnoughReplicas
  | NotEnoughReplicasAfterAppend
  | InvalidRequiredAcks
  | IllegalGeneration
  | InconsistentGroupProtocol
  | InvalidGroupId
  | UnknownMemberId
  | InvalidSessionTimeout
  | RebalanceInProgress
  | InvalidCommitOffsetSize
  | TopicAuthorizationFailed
  | GroupAuthorizationFailed
  | ClusterAuthorizationFailed
  deriving (Show, Eq, Generic)

retriable :: ErrorCode -> Bool
retriable c = case c of
  CorruptMessage -> True
  UnknownTopicOrPartition -> True
  LeaderNotAvailable -> True
  NotLeaderForPartition -> True
  RequestTimedOut -> True
  GroupLoadInProgress -> True
  GroupCoordinatorNotAvailable -> True
  NotCoordinatorForGroup -> True
  NotEnoughReplicas -> True
  NotEnoughReplicasAfterAppend -> True
  _ -> False

instance Enum ErrorCode where
  toEnum c = case c of
    0 -> NoError
    1 -> OffsetOutOfRange
    2 -> CorruptMessage
    3 -> UnknownTopicOrPartition
    4 -> InvalidMessageSize
    5 -> LeaderNotAvailable
    6 -> NotLeaderForPartition
    7 -> RequestTimedOut
    8 -> BrokerNotAvailable
    9 -> ReplicaNotAvailable
    10 -> MessageSizeTooLarge
    11 -> StaleControllerEpoch
    12 -> OffsetMetadataTooLarge
    14 -> GroupLoadInProgress
    15 -> GroupCoordinatorNotAvailable
    16 -> NotCoordinatorForGroup
    17 -> InvalidTopic
    18 -> RecordListTooLarge
    19 -> NotEnoughReplicas
    20 -> NotEnoughReplicasAfterAppend
    21 -> InvalidRequiredAcks
    22 -> IllegalGeneration
    23 -> InconsistentGroupProtocol
    24 -> InvalidGroupId
    25 -> UnknownMemberId
    26 -> InvalidSessionTimeout
    27 -> RebalanceInProgress
    28 -> InvalidCommitOffsetSize
    29 -> TopicAuthorizationFailed
    30 -> GroupAuthorizationFailed
    31 -> ClusterAuthorizationFailed
    
    _ -> Unknown
  fromEnum c = case c of
    NoError                      -> 0
    Unknown                      -> -1
    OffsetOutOfRange             -> 1
    CorruptMessage               -> 2
    UnknownTopicOrPartition      -> 3
    InvalidMessageSize           -> 4
    LeaderNotAvailable           -> 5
    NotLeaderForPartition        -> 6
    RequestTimedOut              -> 7
    BrokerNotAvailable           -> 8
    ReplicaNotAvailable          -> 9
    MessageSizeTooLarge          -> 10
    StaleControllerEpoch         -> 11
    OffsetMetadataTooLarge       -> 12
    GroupLoadInProgress          -> 14
    GroupCoordinatorNotAvailable -> 15
    NotCoordinatorForGroup       -> 16
    InvalidTopic                 -> 17
    RecordListTooLarge           -> 18
    NotEnoughReplicas            -> 19
    NotEnoughReplicasAfterAppend -> 20
    InvalidRequiredAcks          -> 21
    IllegalGeneration            -> 22
    InconsistentGroupProtocol    -> 23
    InvalidGroupId               -> 24
    UnknownMemberId              -> 25
    InvalidSessionTimeout        -> 26
    RebalanceInProgress          -> 27
    InvalidCommitOffsetSize      -> 28
    TopicAuthorizationFailed     -> 29
    GroupAuthorizationFailed     -> 30
    ClusterAuthorizationFailed   -> 31

instance Binary ErrorCode where
  get = (toEnum . fromIntegral) <$> (get :: Get Int16)
  put = (\x -> put (x :: Int16)) . fromIntegral . fromEnum

instance ByteSize ErrorCode where
  byteSize = const 2


newtype ApiVersion = ApiVersion { fromApiVersion :: Int16 }
  deriving (Show, Eq, Ord, Binary, ByteSize, Num)


newtype CoordinatorId = CoordinatorId { fromCoordinatorId :: Int32 }
  deriving (Show, Eq, Ord, Binary, ByteSize)


newtype NodeId = NodeId { fromNodeId :: Int32 }
  deriving (Show, Eq, Ord, Binary, ByteSize, Integral, Enum, Real, Num)


newtype PartitionId = PartitionId { fromPartitionId :: Int32 }
  deriving (Show, Eq, Ord, Binary, ByteSize)

newtype ConsumerId = ConsumerId { fromConsumerId :: Utf8 }
  deriving (Show, Eq, Binary, ByteSize, Hashable)


data CompressionCodec = NoCompression
                      | GZip
                      | Snappy
                      deriving (Show, Eq)


newtype Attributes = Attributes { attributesCompression :: CompressionCodec }
  deriving (Show, Eq)

instance ByteSize Attributes where
  byteSize = const 1

instance Binary Attributes where
  get = do
    x <- get :: Get Int8
    return $ Attributes $! case x of
      0 -> NoCompression
      1 -> GZip
      2 -> Snappy
      _ -> error "Unsupported compression codec"
  put (Attributes c) = do
    let x = case c of
          NoCompression -> 0
          GZip          -> 1
          Snappy        -> 2
    put (x :: Int8)

data Message = Message
  -- messageCrc would go here
  { messageAttributes :: !Attributes
  , messageKey        :: !Bytes
  , messageValue      :: !Bytes
  } deriving (Show, Eq, Generic)

makeFields ''Message

instance ByteSize Message where
  byteSize m = 5 + -- crc, magic byte
               byteSize (messageAttributes m) +
               byteSize (messageKey m) +
               byteSize (messageValue m)
  {-# INLINE byteSize #-}

-- TODO, make crc checking optional
instance Binary Message where
  get = do
    crc <- get :: Get Word32
    bsStart <- bytesRead
    let messageGetter = do
          get :: Get Int8
          msg <- Message <$> get <*> get <*> get
          bsEnd <- bytesRead
          return (msg, bsEnd)
    (msg, bsEnd) <- lookAhead messageGetter
    crcChunk <- getByteString $ fromIntegral (bsEnd - bsStart)
    let crcFinal = crc32 crcChunk
    if crcFinal /= crc
      then fail ("Invalid CRC: expected " ++ show crc ++ ", got " ++ show crcFinal)
      else return msg

  put p = do
    let rest = runPut $ do
          put (0 :: Int8)
          putL attributes p
          put $ messageKey p
          put $ messageValue p
        msgCrc = crc32 rest
    put msgCrc
    putLazyByteString rest


data MessageSetItem = MessageSetItem
  { messageSetItemOffset  :: !Int64
  , messageSetItemMessage :: !Message
  } deriving (Show, Eq, Generic)

makeFields ''MessageSetItem

instance Binary MessageSetItem where
  get = do
    o <- get
    s <- get :: Get Int32
    m <- isolate (fromIntegral s) get
    return $ MessageSetItem o m
  {-# INLINE get #-}

  put i = do
    putL offset i
    putL (message . to byteSize) i
    putL message i
  {-# INLINE put #-}

instance ByteSize MessageSetItem where
  byteSize m = byteSize (messageSetItemOffset m) +
               4 + -- MessageSize
               byteSize (messageSetItemMessage m)
  {-# INLINE byteSize #-}

type MessageSet = [MessageSetItem]

messageSetByteSize :: MessageSet -> Int32
messageSetByteSize = sum . map byteSize

getMessageSet :: Int32 -> Get MessageSet
getMessageSet c = fmap reverse $ isolate (fromIntegral c) $ go
  where
    go = do
      rs <- firstPass []
      case rs of
        [m] -> case m of
          -- handle decompression and return inner message set
          (MessageSetItem _ (Message (Attributes GZip) _ (Bytes v))) ->
            let decompressed = GZip.decompress v in
            case runGetOrFail (getMessageSet $ fromIntegral $ L.length decompressed) decompressed of
              Left (_, _, err) -> fail err
              -- TODO, double reverse and unwrapping is really gross
              Right (_, _, rs') -> return $ reverse rs'
          _ -> return rs
        _ -> return rs
    firstPass xs = do
      pred <- isEmpty
      if pred
        then return xs
        else do
          mx <- optional get
          case mx of
            Nothing -> do
              consumed <- bytesRead
              let rem = c - fromIntegral consumed
              void $ getByteString $ fromIntegral rem
              return xs
            Just x -> firstPass (x:xs)

putMessageSet :: MessageSet -> Put
putMessageSet = mapM_ put 


newtype GenerationId = GenerationId Int32
  deriving (Eq, Show, Binary, ByteSize)

newtype RetentionTime = RetentionTime Int64
  deriving (Eq, Show, Binary, ByteSize)

data Response resp = Response
  { responseCorrelationId :: !CorrelationId
  , responseMessage       :: !resp
  } deriving (Show, Eq, Generic)

makeFields ''Response

instance (ByteSize resp, Binary resp) => Binary (Response resp) where
  get = Response <$> get <*> get
  put p = do
    put $ responseCorrelationId p
    put $ responseMessage p

instance (ByteSize resp) => ByteSize (Response resp) where
  byteSize p = byteSize (responseCorrelationId p) +
               byteSize (responseMessage p)

instance (ByteSize req, RequestApiVersion req) => ByteSize (Request req) where
  byteSize p = byteSize (undefined :: ApiKey) +
               byteSize (apiVersion p) +
               byteSize (requestCorrelationId p) +
               byteSize (requestClientId p) +
               byteSize (requestMessage p)

newMessage :: Message -> MessageSetItem
newMessage = MessageSetItem 0

uncompressed :: L.ByteString -> L.ByteString -> MessageSetItem
uncompressed k v = newMessage $ Message (Attributes NoCompression) (Bytes k) (Bytes v)

gzipCompressed :: MessageSet -> MessageSet
gzipCompressed = (:[]) .  newMessage . gzippedMessage

gzippedMessage :: MessageSet -> Message
gzippedMessage = Message (Attributes GZip) (Bytes "") .
                 Bytes .
                 GZip.compress .
                 runPut .
                 putMessageSet

snappyCompressed :: L.ByteString -> L.ByteString -> Message
snappyCompressed k v = error "Not implemented"

data RecordMetadata = RecordMetadata
  { recordMetadataOffset    :: Int64
  , recordMetadataPartition :: PartitionId
  , recordMetadataTopic     :: Utf8
  } deriving (Show, Eq, Generic)

makeFields ''RecordMetadata
