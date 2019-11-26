module Runner where

import Hello
import World
import Storage

run :: IO ()
run = do
  putStrLn (Hello.f ++ " " ++ World.w)
  loadMessages -- Обрабатываем сообщения.



-- Consumer

-- 1) Describe how messages are processed
-- 2) Accesspts a batch of messages of any type (function decides what to do with undefined message we cannot proccess)
-- 3) It uses writer to write message to event store

-- Hasql reader

-- 1) Connect to database
-- 2) Read the message

-- Hasql writer

-- 1) Accepts the message/messages
-- 2) Writes them in particular stream
