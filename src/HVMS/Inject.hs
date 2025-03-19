module HVMS.Inject where

import Control.Monad (foldM)
import Data.Word
import Foreign.C.String (newCString)
import qualified Data.Map as Map
import HVMS.Type

type VarMap = Map.Map String Loc

-- Maps definition string names to their (eventual) index in the book.
type DefMap = Map.Map String Word32


-- Core to Runtime
-- --------------

injectPCore :: PCore -> Maybe Loc -> DefMap -> VarMap -> IO (VarMap, Term)
injectPCore (PVar name) loc defs vars = case (Map.lookup name vars, loc) of
  -- temporary node that will be mutated when the negative end of this variable is injected.
  (Nothing, Just pos_loc) -> return (Map.insert name pos_loc vars, termNew NUL 0 0)
  (Just neg_loc, _      ) -> return (Map.delete name vars, termNew VAR 0 neg_loc)
  _                       -> do
    putStrLn $ "injectPCore: invalid VAR (" ++ name ++ ")"
    return (vars, _VOID_)
injectPCore (PRef name) loc defs vars = case Map.lookup name defs of
  Just idx -> return (vars, termNew REF 0 idx)
  Nothing -> do
    putStrLn $ "injectPCore: unknown definition (" ++ name ++ ")"
    return (vars, _VOID_)
injectPCore PNul _ defs vars =
  return (vars, termNew NUL 0 0)
injectPCore (PLam var bod) loc defs vars = do
  lam <- allocNode 2
  (vars, var) <- injectNCore var (Just (lam + 0)) defs vars
  (vars, bod) <- injectPCore bod (Just (lam + 1)) defs vars
  set (lam + 0) var
  set (lam + 1) bod
  return (vars, termNew LAM 0 lam)
injectPCore (PSup tm1 tm2) loc defs vars = do
  sup <- allocNode 2
  (vars, tm1) <- injectPCore tm1 (Just (sup + 0)) defs vars
  (vars, tm2) <- injectPCore tm2 (Just (sup + 1)) defs vars
  set (sup + 0) tm1
  set (sup + 1) tm2
  return (vars, termNew SUP 0 sup)
injectPCore (PU32 num) loc defs vars =
  return (vars, termNew U32 0 num)
injectPCore (PI32 num) loc defs vars =
  return (vars, termNew I32 0 (int32ToWord32 num))
injectPCore (PF32 num) loc defs vars =
  return (vars, termNew F32 0 (floatToWord32 num))

injectNCore :: NCore -> Maybe Loc -> DefMap -> VarMap -> IO (VarMap, Term)
injectNCore (NSub name) loc defs vars = case (Map.lookup name vars, loc) of
  (Just pos_loc, Just neg_host) -> do
    -- we've seen the positive end of this var already, so we should mutate
    -- it to hold the SUB's location.
    set pos_loc (termNew VAR 0 neg_host)
    return (Map.delete name vars, termNew SUB 0 0)
  -- we haven't seen the positive end of this var yet, so we save the
  -- SUB's location so the VAR can reference it.
  (Nothing, Just neg_host)      -> return (Map.insert name neg_host vars, termNew SUB 0 0)
  _                             -> do
    putStrLn $ "injectNCore: invalid SUB (" ++ name ++ ")"
    return (vars, _VOID_)
injectNCore NEra _ defs vars =
  return (vars, termNew ERA 0 0)
injectNCore (NApp arg ret) _ defs vars = do
  app <- allocNode 2
  (vars, arg) <- injectPCore arg (Just (app + 0)) defs vars
  (vars, ret) <- injectNCore ret (Just (app + 1)) defs vars
  set (app + 0) arg
  set (app + 1) ret
  return (vars, termNew APP 0 app)
injectNCore (NOp2 opr arg ret) _ defs vars = do
  loc <- allocNode 2
  (vars, arg) <- injectPCore arg (Just (loc + 0)) defs vars
  (vars, ret) <- injectNCore ret (Just (loc + 1)) defs vars
  set (loc + 0) arg
  set (loc + 1) ret
  return (vars, termNew OPX (fromIntegral $ fromEnum opr) loc)
injectNCore (NDup dp1 dp2) _ defs vars = do
  dup <- allocNode 2
  (vars, dp1) <- injectNCore dp1 (Just (dup + 0)) defs vars
  (vars, dp2) <- injectNCore dp2 (Just (dup + 1)) defs vars
  set (dup + 0) dp1
  set (dup + 1) dp2
  return (vars, termNew DUP 0 dup)
injectNCore (NMat ret arms) _ defs vars = do
  let num = fromIntegral (length arms) :: Lab
  mt0 <- allocNode 1
  (vars, ret) <- injectNCore ret (Just mt0) defs vars
  set mt0 ret
  mt1 <- allocNode (fromIntegral num :: Word64)
  vars <- foldM (\vars (i, arm) -> do {
    (vars, arm) <- injectPCore arm (Just (mt1 + i)) defs vars;
    set (mt1 + i) arm;
    return vars
  }) vars (zip [0..num-1] arms)
  return (vars, termNew MAT num mt0)

-- Dex, Net, and Book Injection
-- ----------------------------

injectDex :: Dex -> DefMap -> VarMap -> IO VarMap
injectDex (neg, pos) defs vars = do
  (vars, neg) <- injectNCore neg Nothing defs vars
  (vars, pos) <- injectPCore pos Nothing defs vars
  rbagPush neg pos
  return vars

injectNet :: Net -> DefMap -> VarMap -> IO (VarMap, Term)
injectNet (Net root' bag) defs vars = do
  root_loc <- allocNode 1
  (vars, root) <- injectPCore root' (Just root_loc) defs vars
  vars <- foldr (\dex acc -> acc >>= injectDex dex defs) (return vars) bag
  root' <- get root_loc

  if root' == 0 then do
    set root_loc root
    return (vars, root)
  else
    return (vars, root')

injectDef :: DefMap -> String -> Net -> IO ()
injectDef defmap name net = do
  injectNet net defmap Map.empty
  name <- newCString name
  defNew name
  return ()

-- Main Entry Point
-- ----------------

-- Injects the book and returns the (Ref "@main") term.
doInjectBook :: Book -> IO (Maybe Term)
doInjectBook (Book defs) = do
  let defmap = Map.fromList $ zip (Map.keys defs) [0..]

  Map.traverseWithKey (injectDef defmap) defs

  return $ fmap (termNew REF 0) (Map.lookup "main" defmap)
