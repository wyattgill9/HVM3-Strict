{ pkgs ? import (fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/nixos-unstable.tar.gz";
  }) {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    haskell.compiler.ghc912
    haskellPackages.cabal-install
  ];
}

