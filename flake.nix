{
  description = "Postgres indexing utilities to implement high-throughput queries with on-the-fly deduplication";

  inputs = {
    nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.xz";

    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
  };

  outputs = inputs:
    inputs.flake-parts.lib.mkFlake {inherit inputs;} {
      perSystem = {pkgs, ...}: let
        currentPython = pkgs.python311;
      in {
        packages.dedup-pg = currentPython.buildPythonPackage {
          pname = "lsh-postgres";
          version = "0.1.0";

          src = ./.;

          nativeBuildInputs = with pkgs; [
            currentPython
            currentPython.pkgs.numpy
            uv
          ];

          buildPhase = ''
            uv sync
            uv pip install . --no-user
          '';
        };

        devShells.default = pkgs.mkShellNoCC {
          packages = with pkgs; [
            currentPython
            currentPython.pkgs.numpy
            uv
          ];
        };
      };

      systems = inputs.nixpkgs.lib.systems.flakeExposed;
    };
}
