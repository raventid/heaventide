module Runner where

import Storage

run :: IO ()
run = do
  loadMessages -- Обрабатываем сообщения.
  insertMessage -- Записываем новые ивенты

-- Entity : TODO

  -- 1) Create new entity
  -- 2) Set fields to Maybe (None) values (no default values?)
  -- 3) Call function which accepts previos Entity, applies function to it and returns new Entity
  -- 4) QUESTION: WHERE DO I STORE THIS ENTITY, IN ENV??? IO REF???

-- Projection : TODO
  -- 1) Takes entity
  -- 2) Takes one event
  -- 3) Applies event to entity via some function of `Entity -> params -> Entity` type

-- Consumer : TODO

-- In Haskell implementation consumer would be running in main/run loop and will call 

-- 1) Describe how messages are processed
-- 2) Accesspts a batch of messages of any type (function decides what to do with undefined message we cannot proccess)
-- 3) It uses writer to write message to event store


-- Handler : TODO

-- Handler is not a pure function, handler has to write some logs, Functions called from handelr might be pure?
-- Handler accept just one particular type of message + we have some general purpose component which distributes data accross handler in one particular process
-- First implementation might just implement sequential processing without any problem


-- Hasql reader : *IMPLEMENTED*

-- 1) Connect to database
-- 2) Read the message

-- Hasql writer : TODO

-- 1) Accepts the message/messages
-- 2) Writes them in particular stream
