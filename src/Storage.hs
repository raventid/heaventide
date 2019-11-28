{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module Storage where

import Prelude
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


import Data.Char (chr)

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
  flightInfoBookingToken :: String
  , time :: String
  , value :: String
  , old_value :: String
  , sequence :: String
  , segmentIndex :: String
  , processedTime :: String
  } deriving (Show, Generic)

instance A.FromJSON FlightNumberChanged
instance A.ToJSON FlightNumberChanged

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

-- main :: IO ()
-- main = do
--   Right connection <- Connection.acquire connectionSettings
--   result <- Session.run (sumAndDivModSession 3 8 3) connection
--   print result
--   where
--     connectionSettings = Connection.settings "localhost" 5432 "postgres" "" "postgres"
connect :: IO ()
connect = do
  Right connection <- Connection.acquire connectionSettings
  result <- Session.run (sumAndDivModSession 3 8 3) connection
  print result

loadMessages :: IO ()
loadMessages = do
  Right connection <- Connection.acquire connectionSettings
  result <- Session.run loadMessages' connection
  -- V.mapM_
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

convertDecoder :: (Either String FlightNumberChanged, B.ByteString) -> Either Text FlightNumberChanged
convertDecoder ((Left s), b) = Left (pack $ map (chr . fromEnum) (B.unpack b))
convertDecoder ((Right v), _) = Right v

decodeFlightNumberChange :: B.ByteString -> (Either String FlightNumberChanged, B.ByteString)
decodeFlightNumberChange b = (A.eitherDecodeStrict b :: Either String FlightNumberChanged, b)

-- Session with loading messages from database.
loadMessages' :: Session (V.Vector (TUUID, TSTREAM_NAME, TType, TPosition, TGlobalPosition, TData, TMetadata, TTime))
loadMessages' = Session.statement 10 loadMessages''

loadMessages'' :: Statement Int32 (V.Vector (TUUID, TSTREAM_NAME, TType, TPosition, TGlobalPosition, TData, TMetadata, TTime))
loadMessages'' = let
  sql =
    "select id, stream_name, type, position, global_position, data, metadata, time from messages where id <> '4c8dcf04-102c-4d5d-b94a-1faf52ad3526'"
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


-- * Sessions
--
-- Session is an abstraction over the database connection and all possible errors.
-- It is used to execute statements.
-- It is composable and has a Monad instance.
--
-- It's recommended to define sessions in a dedicated 'Sessions'
-- submodule of your project.
-------------------------

sumAndDivModSession :: Int64 -> Int64 -> Int64 -> Session (Int64, Int64)
sumAndDivModSession a b c = do
  -- Get the sum of a and b
  sumOfAAndB <- Session.statement (a, b) sumStatement
  -- Divide the sum by c and get the modulo as well
  Session.statement (sumOfAAndB, c) divModStatement


-- * Statements
--
-- Statement is a definition of an individual SQL-statement,
-- accompanied by a specification of how to encode its parameters and
-- decode its result.
--
-- It's recommended to define statements in a dedicated 'Statements'
-- submodule of your project.
-------------------------

sumStatement :: Statement (Int64, Int64) Int64
sumStatement = Statement sql encoder decoder True where
  sql = "select $1 + $2"
  encoder =
    (fst >$< Encoders.param (Encoders.nonNullable Encoders.int8)) <>
    (snd >$< Encoders.param (Encoders.nonNullable Encoders.int8))
  decoder = Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8))

divModStatement :: Statement (Int64, Int64) (Int64, Int64)
divModStatement = Statement sql encoder decoder True where
  sql = "select $1 / $2, $1 % $2"
  encoder =
    (fst >$< Encoders.param (Encoders.nonNullable Encoders.int8)) <>
    (snd >$< Encoders.param (Encoders.nonNullable Encoders.int8))
  decoder = Decoders.singleRow row where
    row =
      (,) <$>
      Decoders.column (Decoders.nonNullable Decoders.int8) <*>
      Decoders.column (Decoders.nonNullable Decoders.int8)
