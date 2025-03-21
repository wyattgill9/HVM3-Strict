module Main where

import Control.Monad (when)
import Extract
import Inject
import Parse
import Show
import System.CPUTime
import System.Environment (getArgs)
import System.Exit (exitWith, ExitCode(ExitSuccess, ExitFailure))
import System.IO (readFile, print)
import Text.Parsec
import Type

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
  -- Only count time for the interactions, not parsing and memory alloc
  startInitTime <- getCPUTime
  hvmInit

  code <- readFile filePath
  book <- case doParseBook code of
    Right book -> pure book
    Left  err -> exitWithError ("ParserError: " ++ err)

  maybeMain <- doInjectBook book
  main <- case maybeMain of
    Just main -> pure main
    Nothing -> exitWithError "missing 'main' definition"
 
  endInitTime <- getCPUTime
  startNormalizeTime <- getCPUTime

  term <- normalize main

  endNormalizeTime <- getCPUTime
  
  startExtractTime <- getCPUTime
  net <- extractNet term
  putStrLn $ netToString net

  when showStats $ do
    endExtractTime <- getCPUTime
    let initTime = fromIntegral (endInitTime - startInitTime) / (10^9) :: Double
    let normalizeTime = fromIntegral (endNormalizeTime - startNormalizeTime) / (10^9) :: Double
    let extractTime = fromIntegral (endExtractTime - startExtractTime) / (10^9) :: Double
    let totalTime = initTime + normalizeTime + extractTime
    itr <- incItr
    len <- rnodEnd
    let mips = (fromIntegral itr / 1000000.0) / (((normalizeTime + extractTime ))/ 1000.0)
    putStrLn $ "ITRS: " ++ show itr
    putStrLn $ "INIT-TIME: " ++ show initTime ++ "ms"
    putStrLn $ "NORMALIZE-TIME: " ++ show normalizeTime ++ "ms"
    putStrLn $ "EXTRACT-TIME: " ++ show extractTime ++ "ms"
    putStrLn $ "TOTAL-TIME: " ++ show totalTime ++ "ms"
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
