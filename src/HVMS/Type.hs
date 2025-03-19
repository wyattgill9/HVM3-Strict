module HVMS.Type where

import Data.Word
import GHC.Float
import GHC.Int
import Foreign.C.String
import qualified Data.Map.Strict as MS


-- Core Types
-- ----------

-- A Term is a tree of IN nodes, ending in variables (aux wires)
data PCore
  = PVar String
  | PRef String
  | PNul
  | PLam NCore PCore
  | PSup PCore PCore
  | PU32 Word32
  | PI32 Int32
  | PF32 Float
  deriving (Show, Eq)

data NCore
  = NSub String
  | NEra
  | NApp PCore NCore
  | NOp2 Oper  PCore NCore
  | NDup NCore NCore
  | NMat NCore [PCore]
  deriving (Show, Eq)

data Oper
  = OP_ADD | OP_SUB | OP_MUL | OP_DIV
  | OP_MOD | OP_EQ  | OP_NE  | OP_LT
  | OP_GT  | OP_LTE | OP_GTE | OP_AND
  | OP_OR  | OP_XOR | OP_LSH | OP_RSH
  deriving (Show, Eq, Enum)

-- A Redex is a pair of Terms (trees connected by their main ports)
type Dex = (NCore, PCore)

-- A Bag is a list of redexes
type Bag = [Dex]

-- A Net is a Bag, plus a root Term
data Net = Net
  { netRoot :: PCore
  , netBag  :: Bag
  } deriving (Show, Eq)

-- A Book is a set of definitions
data Book = Book
  { defs :: MS.Map String Net
  } deriving (Show, Eq)

-- Runtime Types
-- -------------

type Tag_ = Word8
type Lab  = Word32
type Loc  = Word32
type Term = Word64

data Tag
  = VOID
  | VAR
  | SUB
  | NUL
  | ERA
  | LAM
  | APP
  | SUP
  | DUP
  | REF
  | OPX
  | OPY
  | U32
  | I32
  | F32
  | MAT
  deriving (Enum, Eq, Show)

-- Runtime constants

_VOID_ :: Term
_VOID_ = 0x0

-- FFI imports
foreign import ccall unsafe "Runtime.c hvm_init"
  hvmInit :: IO ()

foreign import ccall unsafe "Runtime.c hvm_free"
  hvmFree :: IO ()

foreign import ccall unsafe "Runtime.c term_new"
  termNew_ :: Tag_ -> Lab -> Loc -> Term
termNew :: Tag -> Lab -> Loc -> Term
termNew tag lab loc = termNew_ (fromIntegral (fromEnum tag) :: Tag_) lab loc

foreign import ccall unsafe "Runtime.c term_tag"
  termTag_ :: Term -> Tag_
termTag :: Term -> Tag
termTag term = toEnum $ (fromIntegral (termTag_ term))

foreign import ccall unsafe "Runtime.c term_lab"
  termLab :: Term -> Lab

foreign import ccall unsafe "Runtime.c term_loc"
  termLoc :: Term -> Loc

foreign import ccall unsafe "Runtime.c def_new"
  defNew :: CString -> IO ()

foreign import ccall unsafe "Runtime.c def_name"
  defName_ :: Loc -> IO CString
defName :: Loc -> IO String
defName loc = defName_ loc >>= peekCString

foreign import ccall unsafe "Runtime.c swap"
  swap :: Loc -> Term -> IO Term

foreign import ccall unsafe "Runtime.c get"
  get :: Loc -> IO Term

foreign import ccall unsafe "Runtime.c set"
  set :: Loc -> Term -> IO ()

foreign import ccall unsafe "Runtime.c rbag_push"
  rbagPush :: Term -> Term -> IO ()

foreign import ccall unsafe "Runtime.c rbag_ini"
  rbagIni :: IO Loc

foreign import ccall unsafe "Runtime.c rbag_end"
  rbagEnd :: IO Loc

foreign import ccall unsafe "Runtime.c rnod_end"
  rnodEnd :: IO Loc

-- foreign import ccall unsafe "Runtime.c take"
--   take :: Loc -> IO Term

foreign import ccall unsafe "Runtime.c alloc_node"
  allocNode :: Word64 -> IO Loc

foreign import ccall unsafe "Runtime.c inc_itr"
  incItr :: IO Word64

foreign import ccall unsafe "Runtime.c normalize"
  normalize :: Term -> IO Term

foreign import ccall unsafe "Runtime.c dump_buff"
  dumpBuff :: IO ()

-- Convenience Functions

termOper :: Term -> Oper
termOper term = (toEnum (fromIntegral (termLab term)))

foreign import ccall unsafe "Runtime.c u32_to_i32"
  word32ToInt32 :: Word32 -> Int32

foreign import ccall unsafe "Runtime.c u32_to_f32"
  word32ToFloat :: Word32 -> Float

foreign import ccall unsafe "Runtime.c i32_to_u32"
  int32ToWord32 :: Int32 -> Word32

foreign import ccall unsafe "Runtime.c f32_to_u32"
  floatToWord32 :: Float -> Word32
