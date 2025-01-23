{
  description = "Sync tests for cardano-node and db-sync";

  inputs = {
    cardano-node = {
      url = "github:input-output-hk/cardano-node";
    };
    nixpkgs.follows = "cardano-node/nixpkgs";
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
  };

  outputs = { self, nixpkgs, flake-utils, cardano-node }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          devShells = rec {
            base = pkgs.mkShell {
              nativeBuildInputs = with pkgs; [ bash nix gnugrep gnutar coreutils git xz ];
            };

            python = pkgs.mkShell {
              nativeBuildInputs = with pkgs; with python311Packages; [ python311Full virtualenv pip postgresql_14 wget curl ];
              shellHook = ''
                echo "Setting up Python environment..."
                python3 -m venv .venv || true
                source .venv/bin/activate
                .venv/bin/pip install -e .
                echo "Python environment ready."
              '';
            };

            postgres = pkgs.mkShell {
              nativeBuildInputs = with pkgs; [ glibcLocales postgresql lsof procps wget ];
            };
          };
        });
}
