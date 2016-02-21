module ConnectionTests where
import Network.Kafka.Internal.Connection

-- create kafka connection honors configured connection timeout 
-- new kafka connection has no leftovers
-- new kafka connection has no pending responses

-- send causes new response promise to be added
-- send creates new correlation id
-- send causes responses to flush if too many are in flight

-- sendNoResponse doesn't enqueue request to be fulfilled.

-- recv can parse all API response types
-- recv uses unconsumed data as part of next recv call
-- recv causes responses from previous sends to be fulfilled with correct responses
-- recv causes fulfilled promises to be removed from connection

-- reconnect triggers errors on unfulfilled requests
-- reconnect drops pending requests from tracked queue
-- reconnect drops leftovers
-- reconnect prevents connections from sending to bad socket while reconnecting
-- reconnect throws specialized exception on reconnection failure
-- reconnect honors configured connection timeout

