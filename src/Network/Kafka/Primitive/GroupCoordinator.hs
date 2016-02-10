{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies          #-}
module Network.Kafka.Primitive.GroupCoordinator where
import qualified Data.Vector as V
import           Control.Lens
import           Network.Kafka.Exports
import           Network.Kafka.Types

newtype GroupCoordinatorRequestV0 = GroupCoordinatorRequestV0
  { groupCoordinatorRequestV0ConsumerGroup :: Utf8
  } deriving (Show, Eq, Generic)

instance Binary GroupCoordinatorRequestV0

instance ByteSize GroupCoordinatorRequestV0 where
  byteSize = byteSize . groupCoordinatorRequestV0ConsumerGroup

data GroupCoordinatorResponseV0 = GroupCoordinatorResponseV0
  { groupCoordinatorResponseV0ErrorCode       :: !ErrorCode
  , groupCoordinatorResponseV0CoordinatorId   :: !CoordinatorId
  , groupCoordinatorResponseV0CoordinatorHost :: !Utf8
  , groupCoordinatorResponseV0CoordinatorPort :: !Int32
  } deriving (Show, Eq, Generic)


instance Binary GroupCoordinatorResponseV0

instance ByteSize GroupCoordinatorResponseV0 where
  byteSize r = byteSize (groupCoordinatorResponseV0ErrorCode r) +
               byteSize (groupCoordinatorResponseV0CoordinatorId r) +
               byteSize (groupCoordinatorResponseV0CoordinatorHost r) +
               byteSize (groupCoordinatorResponseV0CoordinatorPort r)

instance RequestApiKey GroupCoordinatorRequestV0 where
  apiKey = theApiKey 10

instance RequestApiVersion GroupCoordinatorRequestV0 where
  apiVersion = const 0

