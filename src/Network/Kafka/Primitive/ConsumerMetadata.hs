{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
module Network.Kafka.Primitive.ConsumerMetadata where
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data instance RequestMessage ConsumerMetadata 0 = ConsumerMetadataRequest
  { consumerMetadataRequestConsumerGroup :: !Utf8
  } deriving (Show, Generic)

instance Binary (RequestMessage ConsumerMetadata 0) where
  get = ConsumerMetadataRequest <$> get
  put = put . consumerMetadataRequestConsumerGroup

instance ByteSize (RequestMessage ConsumerMetadata 0) where
  byteSize = byteSize . consumerMetadataRequestConsumerGroup

instance HasConsumerGroup (RequestMessage ConsumerMetadata 0) Utf8 where
  consumerGroup = lens consumerMetadataRequestConsumerGroup (\s a -> s { consumerMetadataRequestConsumerGroup = a })
  {-# INLINEABLE consumerGroup #-}


data instance ResponseMessage ConsumerMetadata 0 = ConsumerMetadataResponse
  { consumerMetadataResponseV0ErrorCode       :: !ErrorCode
  , consumerMetadataResponseV0CoordinatorId   :: !CoordinatorId
  , consumerMetadataResponseV0CoordinatorHost :: !Utf8
  , consumerMetadataResponseV0CoordinatorPort :: !Int32
  } deriving (Show, Generic)

instance Binary (ResponseMessage ConsumerMetadata 0) where
  get = ConsumerMetadataResponse <$> get <*> get <*> get <*> get 
  put r = do
    putL errorCode r
    putL coordinatorId r
    putL coordinatorHost r
    putL coordinatorPort r

instance ByteSize (ResponseMessage ConsumerMetadata 0) where
  byteSize r = byteSize (consumerMetadataResponseV0ErrorCode r) +
               byteSize (consumerMetadataResponseV0CoordinatorId r) +
               byteSize (consumerMetadataResponseV0CoordinatorHost r) +
               byteSize (consumerMetadataResponseV0CoordinatorPort r)

instance HasErrorCode (ResponseMessage ConsumerMetadata 0) ErrorCode where
  errorCode = lens consumerMetadataResponseV0ErrorCode (\s a -> s { consumerMetadataResponseV0ErrorCode = a })
  {-# INLINEABLE errorCode #-}

instance HasCoordinatorId (ResponseMessage ConsumerMetadata 0) CoordinatorId where
  coordinatorId = lens consumerMetadataResponseV0CoordinatorId (\s a -> s { consumerMetadataResponseV0CoordinatorId = a })
  {-# INLINEABLE coordinatorId #-}

instance HasCoordinatorHost (ResponseMessage ConsumerMetadata 0) Utf8 where
  coordinatorHost = lens consumerMetadataResponseV0CoordinatorHost (\s a -> s { consumerMetadataResponseV0CoordinatorHost = a })
  {-# INLINEABLE coordinatorHost #-}

instance HasCoordinatorPort (ResponseMessage ConsumerMetadata 0) Int32 where
  coordinatorPort = lens consumerMetadataResponseV0CoordinatorPort (\s a -> s { consumerMetadataResponseV0CoordinatorPort = a })
  {-# INLINEABLE coordinatorPort #-}
