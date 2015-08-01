module Network.Kafka.Primitive
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put

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
  when (fromIntegral (byteSize x) /= c) $ putStrLn "Invalid byteSize!\n"
  if x /= x'
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
  asserting $ PartitionFetch (PartitionId 9) 789 101112
  asserting $ TopicFetch (Utf8 "topic") $ V.fromList []
  asserting $ FetchRequestV0 (NodeId 5) 7 13 $ V.fromList []
  asserting $ ProduceRequestV0 1 3000 $ V.fromList
    [ TopicPublish (Utf8 "topic") $ V.fromList
        [ PartitionMessages (PartitionId 0) $ MessageSet
            [ MessageSetItem 0 $ Message 0 (Attributes NoCompression) (Bytes "k") (Bytes "v")
            ]
        ]
    ]
  asserting $ r $ ConsumerMetadataRequestV0 $ Utf8 "consumer-group"
