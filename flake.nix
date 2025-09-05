{
  description = "Nix Simple Cache Action";

  nixConfig = {
    extra-substituters = [
      "https://trevnur.cachix.org"
    ];
    extra-trusted-public-keys = [
      "trevnur.cachix.org-1:hBd15IdszwT52aOxdKs5vNTbq36emvEeGqpb25Bkq6o="
    ];
  };

  inputs = {
    systems.url = "systems";
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    utils = {
      url = "github:numtide/flake-utils";
      inputs.systems.follows = "systems";
    };
    nur = {
      url = "github:nix-community/NUR";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    nixpkgs,
    utils,
    nur,
    ...
  }:
    utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [nur.overlays.default];
      };
      trev = pkgs.nur.repos.trev;
      node = pkgs.nodejs_24; # increment as needed
    in rec {
      devShells.default = pkgs.mkShell {
        packages = with pkgs; [
          node
          biome
          prettier

          # utils
          trev.bumper

          # nix
          alejandra
          flake-checker

          # actions
          action-validator
          trev.renovate
        ];
        shellHook = trev.shellhook.ref;
      };

      packages.default = pkgs.buildNpmPackage (finalAttrs: {
        pname = "nix-simple-cache-action";
        version = "1.4.8";
        src = ./.;
        nodejs = node;

        npmDeps = pkgs.importNpmLock {
          npmRoot = ./.;
        };

        npmConfigHook = pkgs.importNpmLock.npmConfigHook;

        nativeBuildInputs = with pkgs; [
          makeWrapper
        ];

        installPhase = ''
          runHook preInstall

          mkdir -p $out/{bin,lib/node_modules/nix-simple-cache-action}
          cp -r dist node_modules package.json $out/lib/node_modules/nix-simple-cache-action

          makeWrapper "${pkgs.lib.getExe node}" "$out/bin/nix-simple-cache-action" \
            --add-flags "$out/lib/node_modules/nix-simple-cache-action/dist/index.js"

          runHook postInstall
        '';

        meta.mainProgram = "nix-simple-cache-action";
      });

      checks =
        trev.lib.mkChecks {
          lint = {
            src = ./.;
            nativeBuildInputs = with pkgs; [
              biome
              prettier
              alejandra
              action-validator
              trev.renovate
            ];
            checkPhase = ''
              biome check .
              prettier --check .
              alejandra -c flake.nix
              action-validator .github/**/*.yaml
              renovate-config-validator .github/renovate*.json
            '';
          };
        }
        // {
          shell = devShells.default;
        };

      formatter = pkgs.alejandra;
    });
}
