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
              nativeBuildInputs = with pkgs; [ bash nix gnugrep gnumake gnutar coreutils git xz ];
            };

            python = pkgs.mkShell {
              nativeBuildInputs = with pkgs; with python39Packages; [ python39Full virtualenv pip matplotlib pandas requests xmltodict psutil GitPython pymysql postgresql_14 wget curl psycopg2 assertpy colorama];
              shellHook = ''
                echo "Setting up Python environment..."
                python3 -m venv .venv || true
                source .venv/bin/activate
                .venv/bin/python -m pip install --upgrade pip
                .venv/bin/pip install pytest requests allure-pytest pytest-html pytest-order pyyaml psycopg2 psutil blockfrost-python GitPython colorama matplotlib
                echo "Python environment ready."
              '';
            };

            postgres = pkgs.mkShell {
              nativeBuildInputs = with pkgs; [ glibcLocales postgresql lsof procps wget ];
            };
          };
        });
}
