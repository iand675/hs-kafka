{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies          #-}
module Network.Kafka.Primitive.ConsumerMetadata where
import qualified Data.Vector as V
import           Network.Kafka.Exports
import           Network.Kafka.Types

data instance RequestMessage ConsumerMetadata 0 = ConsumerMetadataRequestV0
  { consumerMetadataRequestV0ConsumerGroup :: !Utf8
  } deriving (Show, Eq, Generic)

instance Binary (RequestMessage ConsumerMetadata 0) where
  get = ConsumerMetadataRequestV0 <$> get
  put = putL consumerGroup 

instance ByteSize (RequestMessage ConsumerMetadata 0) where
  byteSize = byteSizeL consumerGroup

instance HasConsumerGroup (RequestMessage ConsumerMetadata 0) Utf8 where
  consumerGroup = lens consumerMetadataRequestV0ConsumerGroup (\s a -> s { consumerMetadataRequestV0ConsumerGroup = a })
  {-# INLINEABLE consumerGroup #-}

data instance ResponseMessage ConsumerMetadata 0 = ConsumerMetadataResponseV0
  { consumerMetadataResponseV0ErrorCode       :: !ErrorCode
  , consumerMetadataResponseV0CoordinatorId   :: !CoordinatorId
  , consumerMetadataResponseV0CoordinatorHost :: !Utf8
  , consumerMetadataResponseV0CoordinatorPort :: !Int32
  } deriving (Show, Eq, Generic)

instance Binary (ResponseMessage ConsumerMetadata 0) where
  get = ConsumerMetadataResponseV0 <$> get <*> get <*> get <*> get 
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

