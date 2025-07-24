{
  description = "Sync tests for cardano-node and db-sync";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
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
              nativeBuildInputs = with pkgs; [
                git
                py3Full
                py3Pkgs.virtualenv
                py3Pkgs.pip
                postgresql
                wget
                curl
                ripgrep
                # Install selected Python deps using nix to ensure that
                # all required C libs are present.
                py3Pkgs.matplotlib
                py3Pkgs.numpy
                py3Pkgs.pandas
              ];
              shellHook = ''
                echo "Setting up Python environment..."
                [ -e .nix_venv ] || python3 -m venv .nix_venv
                source .nix_venv/bin/activate
                export PYTHONPATH=$(echo "$VIRTUAL_ENV"/lib/python3*/site-packages):"$PYTHONPATH"
                python3 -m pip install --require-virtualenv --upgrade -e .
                echo "Environment ready."
              '';
            };

            postgres = pkgs.mkShell {
              nativeBuildInputs = with pkgs; [ glibcLocales postgresql lsof procps wget ];
            };

            default = python;
          };
        });

  # --- Flake Local Nix Configuration ----------------------------
  nixConfig = {
    # Sets the flake to use the IOG nix cache.
    extra-substituters = [ "https://cache.iog.io" ];
    extra-trusted-public-keys = [ "hydra.iohk.io:f/Ea+s+dFdN+3Y/G+FDgSq+a5NEWhJGzdjvKNGv0/EQ=" ];
    allow-import-from-derivation = "true";
  };
}
