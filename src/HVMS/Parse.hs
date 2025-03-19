module HVMS.Parse where

import HVMS.Type
import HVMS.Show (operToString)

import Data.Word
import GHC.Int
import GHC.Float
import Text.Read (readMaybe)
import Text.Parsec
import Text.Parsec.String

import Debug.Trace
import qualified Data.Map.Strict as MS
import qualified Control.Applicative as Applicative

-- Core Parser
-- ----------

parsePCore :: Parser PCore
parsePCore = do
  head <- peekNextChar
  case head of
    '*' -> do
      consume "*"
      return PNul
    '(' -> do
      consume "("
      var <- parseNCore
      bod <- parsePCore
      consume ")"
      return $ PLam var bod
    '{' -> do
      consume "{"
      tm1 <- parsePCore
      tm2 <- parsePCore
      consume "}"
      return $ PSup tm1 tm2
    '@' -> do
      consume "@"
      name <- parseName
      return $ PRef name
    _ -> do
      parseNum <|> fmap PVar parseName

parseNCore :: Parser NCore
parseNCore = do
  head <- peekNextChar
  case head of
    '*' -> do
      consume "*"
      return NEra
    '(' -> do
      consume "("
      core <-
        do {
          opr <- parseOper;
          arg <- parsePCore;
          ret <- parseNCore;
          return $ NOp2 opr arg ret
        } <|> do {
          arg <- parsePCore;
          ret <- parseNCore;
          return $ NApp arg ret
        }
      consume ")"
      return core
    '{' -> do
      consume "{"
      dp1 <- parseNCore
      dp2 <- parseNCore
      consume "}"
      return $ NDup dp1 dp2
    '?' -> do
      consume "?("
      ret  <- parseNCore
      arms <- many1 parsePCore
      consume ")"
      return $ NMat ret arms
    _ -> do
      name <- parseName
      return $ NSub name

parseDex :: Parser Dex
parseDex = do
  consume "&"
  neg <- parseNCore
  consume "~"
  pos <- parsePCore
  return (neg, pos)

parseBag :: Parser Bag
parseBag = do
  head <- try peekNextChar <|> return ' '
  case head of
    '&' -> do
      dex <- parseDex
      rest <- parseBag
      return (dex : rest)
    _ -> return []

parseNet :: Parser Net
parseNet = do
  rot <- parsePCore
  bag <- parseBag
  return $ Net rot bag

parseDef :: Parser (String, Net)
parseDef = do
  consume "@"
  name <- parseName
  consume "="
  net <- parseNet
  skip
  return (name, net)

parseBook :: Parser Book
parseBook = do
  defs <- many parseDef
  skip
  eof
  return $ Book (MS.fromList defs)

-- Utilities
-- ---------

peekNextChar :: Parser Char
peekNextChar = skip >> lookAhead anyChar

parseName :: Parser String
parseName = skip >> many1 (alphaNum <|> char '_')

parseOper :: Parser Oper
parseOper = do
  let opers :: [Oper] = enumFrom (toEnum 0)
  let operParser op = string' (operToString op) >> return op
  choice $ map operParser opers

parseNum :: Parser PCore
parseNum = do
  head <- digit <|> oneOf "+-"
  tail <- many (alphaNum <|> oneOf ".-+")
  let num = (head : tail)

  case readMaybeNumeric num of
    Just core -> return core
    Nothing   -> fail $ "Invalid num " ++ show num

readMaybeNumeric :: String -> Maybe PCore
readMaybeNumeric text = do
  let (<|>) = (Applicative.<|>)

  fmap PU32 (readMaybe text :: Maybe Word32) <|>
    fmap PI32 (readMaybe text :: Maybe Int32) <|>
    fmap PF32 (readMaybe text :: Maybe Float)

skip :: Parser ()
skip = skipMany (parseSpace <|> parseComment) where
  parseSpace = (try $ do
    space
    return ()) <?> "space"
  parseComment = (try $ do
    string "//"
    skipMany (noneOf "\n")
    char '\n'
    return ()) <?> "comment"

consume :: String -> Parser String
consume str = skip >> string str

-- Main Entry Point
-- ----------------

doParseBook :: String -> Either String Book
doParseBook code = case parse parseBook "" code of
  Right net -> Right net
  Left  err -> Left  (show err)
