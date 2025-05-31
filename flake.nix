{
  description = "HVM3 flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
                haskell.compiler.ghc912
                haskellPackages.cabal-install 
                haskellPackages.haskell-language-server

                # clang
                # clangd
                # gdb
          ];

          shellHook = ''
            echo "Welcome to the Haskell-C dev shell"
          '';
        };

      });
}
