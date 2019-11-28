module Entity where

data State = State
  {
  bookingToken :: Integer
  , sequence :: Integer
  }


alreadyProcessed :: State -> Integer -> Boolean
alreadyProcessed = (State #. sequence) > Integer
