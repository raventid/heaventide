module Projection where

import qualified Entity as E


-- Кто-то должен сделать здесь эффект за нас, если мы не собирамся тащить сюда логгер
-- Если собираемся логгер тащить, то тогда можно прямо в Hasql тут ходить, но тогда
-- как-то совсем зашкварно получается.
restoreStateFromStorage :: [Events] -> E.State -> E.State
restoreStateFromStorage es e = foldl apply es e

apply :: Event -> E.State -> E.State
apply (Event1 ev) e = e{field1 = ev.field1}
apply (Event2 ev) e = e{field2 = ev.field2}



