{
  description = "Sync tests for cardano-node and db-sync";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          py3Pkgs = pkgs.python311Packages;
          py3Full = pkgs.python311Full;
        in
        {
          devShells = rec {
            base = pkgs.mkShell {
              nativeBuildInputs = with pkgs; [ bash gnugrep gnutar coreutils git xz ];
            };

            python = pkgs.mkShell {
              nativeBuildInputs = with pkgs; [ py3Full py3Pkgs.virtualenv py3Pkgs.pip postgresql_14 wget curl ];
              shellHook = ''
                echo "Setting up Python environment..."
                python3 -m venv .venv_nix || true
                source .venv_nix/bin/activate
                .venv_nix/bin/pip install -e .
                echo "Python environment ready."
              '';
            };

            postgres = pkgs.mkShell {
              nativeBuildInputs = with pkgs; [ glibcLocales postgresql lsof procps wget ];
            };
          };
        });
}
