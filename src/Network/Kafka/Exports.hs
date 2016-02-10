{-# LANGUAGE RankNTypes #-}
module Network.Kafka.Exports
( module Data.Binary
, module Data.Int
, module GHC.TypeLits
, ByteSize(..)
, Generic
) where
import Control.Lens
import Data.Int
import GHC.Generics hiding (to)
import GHC.TypeLits
import Data.Binary

-- | Note that for requests and responses, only NON-COMMON FIELDS should be counted in the instance
class ByteSize a where
  byteSize :: a -> Int32

instance ByteSize Int8 where
  byteSize = const 1

instance ByteSize Int16 where
  byteSize = const 2

instance ByteSize Int32 where
  byteSize = const 4

instance ByteSize Int64 where
  byteSize = const 8

