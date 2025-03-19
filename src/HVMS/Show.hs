module HVMS.Show where

import Data.Word
import Data.List
import Numeric (showHex)
import HVMS.Type

-- Core to String
-- -------------

pcoreToString :: PCore -> String
pcoreToString (PVar nam)     = nam
pcoreToString (PRef nam)     = "@" ++ nam
pcoreToString PNul           = "*"
pcoreToString (PLam var bod) = "(" ++ ncoreToString var ++ " " ++ pcoreToString bod ++ ")"
pcoreToString (PSup t1 t2)   = "{" ++ pcoreToString t1 ++ " " ++ pcoreToString t2 ++ "}"
pcoreToString (PU32 num)     = show num
pcoreToString (PI32 num)     = show num
pcoreToString (PF32 num)     = show num

ncoreToString :: NCore -> String
ncoreToString (NSub nam)        = nam
ncoreToString NEra              = "*"
ncoreToString (NApp arg ret)    = "("  ++ pcoreToString arg ++ " " ++ ncoreToString ret ++ ")"
ncoreToString (NDup d1 d2)      = "{"  ++ ncoreToString d1  ++ " " ++ ncoreToString d2  ++ "}"
ncoreToString (NOp2 op arg ret) = "("  ++ operToString op   ++ " " ++ pcoreToString arg ++ " " ++ ncoreToString ret ++ ")"
ncoreToString (NMat ret arms)   = "?(" ++ ncoreToString ret ++ " " ++ armsString ++ ")"
  where
    armsString = intercalate " " (map pcoreToString arms)

operToString :: Oper -> String
operToString OP_ADD = "+"
operToString OP_SUB = "-"
operToString OP_MUL = "*"
operToString OP_DIV = "/"
operToString OP_MOD = "%"
operToString OP_EQ  = "=="
operToString OP_NE  = "!="
operToString OP_LT  = "<"
operToString OP_GT  = ">"
operToString OP_LTE = "<="
operToString OP_GTE = ">="
operToString OP_AND = "&"
operToString OP_OR  = "|"
operToString OP_XOR = "^"
operToString OP_LSH = "<<"
operToString OP_RSH = ">>"

dexToString :: Dex -> String
dexToString (neg, pos) = ncoreToString neg ++ " ~ " ++ pcoreToString pos

bagToString :: Bag -> String
bagToString [] = ""
bagToString (dex:rest) = "& " ++ dexToString dex ++ "\n" ++ bagToString rest

netToString :: Net -> String
netToString (Net rot bag) = pcoreToString rot ++ "\n" ++ bagToString bag

-- Runtime to String
-- ----------------

labToString :: Lab -> String
labToString lab = padLeft (showHex lab "") 6 '0'

locToString :: Loc -> String
locToString loc = padLeft (showHex loc "") 9 '0'

termToString :: Term -> String
termToString term = show (termTag term) ++ ":" ++ labToString (termLab term) ++ ":" ++ locToString (termLoc term)

-- Utilities
-- ---------

padLeft :: String -> Int -> Char -> String
padLeft str n c = replicate (n - length str) c ++ str
