module ETL where

import Control.Category ((>>>))
import Control.Exception (try, evaluate, SomeException)
import Control.DeepSeq (NFData, force)
import Data.Char (toUpper)
import Control.Monad (void)

newtype Node i o = Node { runNode :: i -> o }
newtype Extract i o = Extract { getExtract :: Node i o }
newtype Transform i o = Transform { getTransform :: Node i o }
newtype Load i o = Load { getLoad :: Node i o }
newtype Pipeline i o = Pipeline { getNode :: Node i o }

instance Functor (Pipeline i) where
  fmap f (Pipeline (Node g)) = Pipeline $ Node (f . g)

instance Applicative (Pipeline i) where
  pure x = Pipeline $ Node $ const x
  Pipeline (Node f) <*> Pipeline (Node g) = Pipeline $ Node $ \i -> f i (g i)

instance Monad (Pipeline i) where
  return = pure
  Pipeline (Node m) >>= f = Pipeline $ Node $ \i -> 
    let a = m i
        Pipeline (Node n) = f a
    in n i

class Sequential n where
  andThen :: n i m -> n m o -> n i o
  
instance Sequential Node where
  andThen (Node f) (Node g) = Node (f >>> g)
  
instance Sequential Extract where
  andThen (Extract f) (Extract g) = Extract (f `andThen` g)
  
instance Sequential Transform where
  andThen (Transform f) (Transform g) = Transform (f `andThen` g)
  
instance Sequential Load where
  andThen (Load f) (Load g) = Load (f `andThen` g)

instance Sequential Pipeline where
  andThen (Pipeline f) (Pipeline g) = Pipeline (f `andThen` g)

class ToNode n where
  toNode :: n i o -> Node i o
  
instance ToNode Node where
  toNode = id
  
instance ToNode Extract where
  toNode (Extract n) = n
  
instance ToNode Transform where
  toNode (Transform n) = n
  
instance ToNode Load where
  toNode (Load n) = n

instance ToNode Pipeline where
  toNode = getNode

extract :: (i -> o) -> Extract i o
extract f = Extract $ Node f

transform :: (i -> o) -> Transform i o
transform f = Transform $ Node f

load :: (i -> o) -> Load i o
load f = Load $ Node f

(~>) :: (ToNode n1, ToNode n2) => n1 i m -> n2 m o -> Pipeline i o
n1 ~> n2 = Pipeline $ toNode n1 `andThen` toNode n2

safeRun :: Pipeline i o -> i -> IO (Either String o)
safeRun (Pipeline (Node f)) input = do
  result <- try $ evaluate $ f input
  return $ case result of
    Left e -> Left $ show (e :: SomeException)
    Right val -> Right val

unsafeRun :: Pipeline i o -> i -> o
unsafeRun (Pipeline (Node f)) = f

-- Example
readName :: Extract String String
readName = extract id

addTitle :: Transform String String
addTitle = transform ("Mr. " ++)

uppercase :: Transform String String
uppercase = transform (map toUpper)

printToConsole :: Load String (IO ())
printToConsole = load putStrLn

titlePipeline :: Pipeline String String
titlePipeline = readName ~> addTitle

upperPipeline :: Pipeline String (IO ())
upperPipeline = readName ~> uppercase ~> printToConsole

combinedPipeline :: Pipeline String String
combinedPipeline = do
  titled <- titlePipeline
  uppered <- upperPipeline
  return $ titled ++ " & " ++ uppered

finalPipeline :: Pipeline String (IO ())
finalPipeline = combinedPipeline ~> printToConsole

-- other examples
readData :: Extract String [Double]
readData = extract (map read . words)

filterNegative :: Transform [Double] [Double]
filterNegative = transform (filter (>0))

addTax :: Transform [Double] [Double]
addTax = transform (map (*1.2))

sumAll :: Transform [Double] Double
sumAll = transform sum

printResult :: Load Double (IO ())
printResult = load print

-- Chain them different ways
basicPipeline :: Pipeline String Double
basicPipeline = readData ~> filterNegative ~> sumAll

taxPipeline :: Pipeline String Double
taxPipeline = readData ~> filterNegative ~> addTax ~> sumAll

-- Usage:
-- > unsafeRun basicPipeline "1.0 2.0 -3.0 4.0"
-- > unsafeRun taxPipeline "1.0 2.0 -3.0 4.0"


main :: IO ()
main = do
  unsafeRun finalPipeline "alice"
