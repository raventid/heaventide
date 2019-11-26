{-# LANGUAGE OverloadedStrings #-}

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

import Data.Text
import qualified Data.Vector as V
import Data.UUID
import Data.Time
import qualified Data.Aeson as A


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
  Right result <- Session.run loadMessages' connection
  V.mapM_ print result

connectionSettings = Connection.settings host port login password database


-- Session with loading messages from database.
loadMessages' :: Session (V.Vector (TUUID, TSTREAM_NAME, TType, TPosition, TGlobalPosition, A.Value, A.Value, TTime))
loadMessages' = Session.statement 10 loadMessages''

-- Statement for loading messages from database
type TUUID = UUID
type TSTREAM_NAME = Text
type TType = Text
type TPosition = Int64
type TGlobalPosition = Int64
-- type TData = JSONB -- Content of message, unique to particular service. How to make decoders and encoders?
-- type TMetadata = JSONB -- Metadata about message is common in message store, maybe move to column?
type TTime = TimeOfDay

loadMessages'' :: Statement Int32 (V.Vector (TUUID, TSTREAM_NAME, TType, TPosition, TGlobalPosition, A.Value, A.Value, TTime))
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
        Decoders.column (Decoders.nonNullable Decoders.jsonb) <*>
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
