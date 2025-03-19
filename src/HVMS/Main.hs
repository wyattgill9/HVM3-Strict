module Main where

import Control.Monad (when)
import System.CPUTime
import System.Environment (getArgs)
import System.Exit (exitWith, ExitCode(ExitSuccess, ExitFailure))
import System.IO (readFile, print)

import Text.Parsec

import HVMS.Type
import HVMS.Inject
import HVMS.Extract
import HVMS.Parse
import HVMS.Show

-- Main
-- ----

main :: IO ()
main = do
  args <- getArgs

  case args of
    ["run", file, "-s"] -> cliRun file True
    ["run", file]       -> cliRun file False
    ["help"]            -> printHelp
    _                   -> printHelp

-- CLI Commands
-- ------------

cliRun :: FilePath -> Bool -> IO ()
cliRun filePath showStats = do
  start <- getCPUTime

  hvmInit

  code <- readFile filePath
  book <- case doParseBook code of
    Right book -> pure book
    Left  err -> exitWithError ("ParserError: " ++ err)

  maybeMain <- doInjectBook book
  main <- case maybeMain of
    Just main -> pure main
    Nothing -> exitWithError "missing 'main' definition"

  term <- normalize main

  net <- extractNet term
  putStrLn $ netToString net

  when showStats $ do
    end <- getCPUTime
    let diff = fromIntegral (end - start) / (10^9) :: Double
    itr <- incItr
    len <- rnodEnd
    let mips = (fromIntegral itr / 1000000.0) / (diff / 1000.0)
    putStrLn $ "ITRS: " ++ show itr
    putStrLn $ "TIME: " ++ show diff ++ "ms"
    putStrLn $ "SIZE: " ++ show len
    putStrLn $ "MIPS: " ++ show mips

  hvmFree

printHelp :: IO ()
printHelp = do
  putStrLn "HVM-Strict usage:"
  putStrLn "  hvms run <file> [-s]  # Normalizes the specified file"
  putStrLn "  hvms help             # Shows this help message"

exitWithError :: String -> IO a
exitWithError msg = do
  putStrLn msg
  exitWith $ ExitFailure 1
