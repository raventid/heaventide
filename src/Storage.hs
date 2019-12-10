{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module Storage where

import Prelude hiding (sequence)
import Data.Int
import Data.Functor.Contravariant
import Hasql.Session (Session)
import Hasql.Statement (Statement(..))
import qualified Hasql.Session as Session
import qualified Hasql.Decoders as Decoders
import qualified Hasql.Encoders as Encoders
import qualified Hasql.Connection as Connection

import Data.Text hiding (map)
import qualified Data.Vector as V
import Data.UUID
import Data.Time
import qualified Data.Aeson as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import GHC.Generics

import Contravariant.Extras.Contrazip


import Data.Char (chr)
import Data.List


-- Common message structure
-- data Message = Message Token StreamName Type Data MetaData


-- Metadata
-- Here, there is a small problem.
-- correlationStreamName is optional and might not present in response and should not be set
-- when we are writing message (because service in another language might not be ready for this)


-- TODO: TMetaData because of collision with Generics package
data TMetaData = TMetaData
  {
    causationMessagePosition :: Integer -- 0
  , causationMessageStreamName :: Text -- "callbackReceiver-123"
  , causationMessageGlobalPosition :: Integer -- 1
  } deriving (Show, Generic)

copyMetaData :: TMetaData -> TMetaData
copyMetaData TMetaData{causationMessagePosition=cmp, causationMessageGlobalPosition=cmgp, causationMessageStreamName=cmsn} = TMetaData{
  causationMessagePosition=cmp
  , causationMessageStreamName=cmsn
  , causationMessageGlobalPosition=cmgp
  }

instance A.ToJSON TMetaData
instance A.FromJSON TMetaData

-- Messages
-- Describe how messages are represented in a system


-- id              | 6d021da6-63b3-4701-8a18-78aaa6ae6b2b
-- stream_name     | flightInfo-00000001-0000-4000-8000-000000000000
-- type            | FlightNumberChanged
-- position        | 0
-- global_position | 2
-- data            | {"time": "2000-01-01T00:00:00.001Z", "value": "3539", "oldValue": "5761", "sequence": 1, "bookingToken": "00000001-0000-4000-8000-000000000000", "segmentIndex": 0, "processedTime": "2019-08-07T19:40:07.697Z"}
-- metadata        | {"causationMessagePosition": 0, "causationMessageStreamName": "callbackReceiver-123", "causationMessageGlobalPosition": 1}
-- time            | 2019-08-07 19:40:07.722982
data FlightNumberChanged = FlightNumberChanged
  {
    bookingToken :: Text
  , time :: Text
  , value :: Text
  , oldValue :: Text
  , sequence :: Integer
  , segmentIndex :: Integer
  , processedTime :: Text
  } deriving (Show, Generic)

instance A.FromJSON FlightNumberChanged
instance A.ToJSON FlightNumberChanged

toMessageFormat :: FlightNumberChanged -> TMetaData -> (Text, TSTREAM_NAME, TType, B.ByteString, B.ByteString)
toMessageFormat flightNC meta = (
  bookingToken flightNC,
  "flightInfo-00000001-0000-4000-8000-000000000000",
  "FlightNumberChanged",
  (LB.toStrict . A.encode) flightNC,
  (LB.toStrict . A.encode) meta
  )

-- Postgresql
-- "idle in transaction"
-- Article "hunting idle in transaction"

-- host :: Data.ByteString.Internal.ByteString
host = "localhost"

-- port :: GHC.Word.Word16
port = 5432

-- database :: Data.ByteString.Internal.ByteString
login = "message_store"

-- uid :: Data.ByteString.Internal.ByteString
password = ""

-- password :: Data.ByteString.Internal.ByteString
database = ""

loadMessages :: IO ()
loadMessages = do
  Right connection <- Connection.acquire connectionSettings
  result <- Session.run loadMessages' connection
  -- V.mapM_
  print result

insertMessage :: IO ()
insertMessage = do
  Right connection <- Connection.acquire connectionSettings
  currentTime <- getCurrentTime

  let flightNC = FlightNumberChanged {
    bookingToken="ae747185-3a4b-40de-86cf-d05897d85d54"
  , time="2000-01-01T00:00:00.001Z"
  , value="4569"
  , oldValue="8957"
  , sequence=50
  , segmentIndex=19
  , processedTime="2000-01-01T00:10:00.001Z"
  }

  let meta = TMetaData {
    causationMessagePosition=0
  , causationMessageStreamName="callbackReceiver-123"
  , causationMessageGlobalPosition=1
  }

-- (utcToLocalTime utc currentTime)
  result <- Session.run (insertMessage' (toMessageFormat flightNC meta)) connection
  print result

connectionSettings = Connection.settings host port login password database

-- Statement for loading messages from database
type TUUID = UUID
type TSTREAM_NAME = Text
type TType = Text
type TPosition = Int64
type TGlobalPosition = Int64
type TData =  FlightNumberChanged
type TMetadata = A.Value
type TTime = TimeOfDay

type TJustJSON = A.Value

-- Some helpers for different string conversions (TODO: Remove all of this and use appropriate types)
dropSohAndConvert :: B.ByteString -> Text
dropSohAndConvert b = pack $ map (chr . fromEnum) $ Data.List.drop 1 $ (B.unpack b)

-- Drop first byte of bytestring
dropSohFromBytestring :: B.ByteString -> B.ByteString
dropSohFromBytestring b = B.drop 1 b

convertDecoder :: (Either String FlightNumberChanged, B.ByteString) -> Either Text FlightNumberChanged
convertDecoder ((Left s), b) = Left (pack s)
convertDecoder ((Right v), _) = Right v

decodeFlightNumberChange :: B.ByteString -> (Either String FlightNumberChanged, B.ByteString)
decodeFlightNumberChange b = (A.eitherDecodeStrict (dropSohFromBytestring b) :: Either String FlightNumberChanged, b)

-- Session with loading messages from database.
loadMessages' :: Session (V.Vector (TUUID, TSTREAM_NAME, TType, TPosition, TGlobalPosition, TData, TMetadata, TTime))
loadMessages' = Session.statement 10 loadMessages''

loadMessages'' :: Statement Int32 (V.Vector (TUUID, TSTREAM_NAME, TType, TPosition, TGlobalPosition, TData, TMetadata, TTime))
loadMessages'' = let
  sql =
    "select id, stream_name, type, position, global_position, data, metadata, time from messages where id = '6d021da6-63b3-4701-8a18-78aaa6ae6b2b'"
  encoder =
    Encoders.param (Encoders.nonNullable Encoders.int4)
  decoder =
    Decoders.rowVector $
      (,,,,,,,) <$>
        Decoders.column (Decoders.nonNullable Decoders.uuid) <*>
        Decoders.column (Decoders.nonNullable Decoders.text) <*>
        Decoders.column (Decoders.nonNullable Decoders.text) <*>
        Decoders.column (Decoders.nonNullable Decoders.int8) <*>
        Decoders.column (Decoders.nonNullable Decoders.int8) <*>
        Decoders.column (Decoders.nonNullable (Decoders.jsonBytes (convertDecoder . decodeFlightNumberChange))) <*>
        Decoders.column (Decoders.nonNullable Decoders.jsonb) <*>
        Decoders.column (Decoders.nonNullable Decoders.time)
  in Statement sql encoder decoder True



insertMessage' :: (Text, TSTREAM_NAME, TType, B.ByteString, B.ByteString) -> Session Int32
insertMessage' (a,b,c,d,e) = Session.statement (a,b,c,d,e) insertMessage''

-- In case of Eventide eventstore I should just call stored procedure here, correct?
-- Insert message into messages table.
-- We will use stored procedure to insert data and get last position for inserted message

-- Here, first param is text, because write message accepts varchar and not uuid as first param
-- TODO: Not sure it will work as expected

-- TODO: Add one more param for EXPECTED_VERSION check (aka optimisstic concurrency)
insertMessage'' :: Statement (Text, TSTREAM_NAME, TType, B.ByteString, B.ByteString) Int32
insertMessage'' = let
  sql = "SELECT write_message($1::varchar, $2::varchar, $3::varchar, $4::jsonb, $5::jsonb);"
  encoder =
    contrazip5
      (Encoders.param (Encoders.nonNullable Encoders.text))
      (Encoders.param (Encoders.nonNullable Encoders.text))
      (Encoders.param (Encoders.nonNullable Encoders.text))
      (Encoders.param (Encoders.nonNullable Encoders.jsonBytes))
      (Encoders.param (Encoders.nonNullable Encoders.jsonBytes))
  decoder =
    Decoders.singleRow ((Decoders.column . Decoders.nonNullable) Decoders.int4)
  in Statement sql encoder decoder True

-- * Sessions
--
-- Session is an abstraction over the database connection and all possible errors.
-- It is used to execute statements.
-- It is composable and has a Monad instance.
--
-- It's recommended to define sessions in a dedicated 'Sessions'
-- submodule of your project.
-------------------------


-- * Statements
--
-- Statement is a definition of an individual SQL-statement,
-- accompanied by a specification of how to encode its parameters and
-- decode its result.
--
-- It's recommended to define statements in a dedicated 'Statements'
-- submodule of your project.
-------------------------


-- Partition schema with Kafka

-- Partition 1
-- Command stream 1 P1 P2 P3
-- Event stream 1 P1 P2 P3

-- Partition 3
-- Command stream 2 P1 P2 P3
-- Event stream 2 P1 P2 P3


-- We could have schema like this:
-- {
--      Partition 1: {
--       command stream 2: ...,
--       event stream 2: ...,
--      }
-- }

-- So, we are not tracking a global position in store, but tracking a position in one partition.
-- TODO: Automatic repartitioning will kill us.



-- In case of Kafka, if we are moving events into ClickHouse
-- Then the schema information will become wrong:
-- {
--      Partition 1: {
--       command stream 2: ...,
--       event stream 2: ...,
--      }
-- }

-- We are tracking sequence number with our own events. So if we are reading another parition like

-- UserStream: ... event_223, event_224, event_225
-- And our stream: ... event_123, event_124


  -- Our last sequence number from stream is 124
-- But UserStream is larger and we will process event_223 and so on, even if they happened before `123`
-- (for example UserStream has a lot of different types of events, but our event stream has just one type of event, in this case we will have much more events in our stream, than in User stream)

-- If we are reinserting data back into Kafka, than we will have incorret data in our partiton schema:
-- I.e. events might have another indexes and it might be another parition.


-- in the same time we might see the same effect with Postgresql storage. If we are reinserting data
-- from another storage and changing global position of those messages, than internal metadata of those messages will become incorrect.


-- Add some more horizontal scaling notes TODO
