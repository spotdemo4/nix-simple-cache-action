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
      url = "github:spotdemo4/nur";
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

      node = pkgs.nodejs_24; # increment as needed
    in {
      devShells = {
        default = pkgs.mkShell {
          packages = with pkgs; [
            node
            biome
            prettier

            # nix
            alejandra
          ];
          shellHook = pkgs.trev.shellhook.ref;
        };

        ci = pkgs.mkShell {
          packages = with pkgs; [
            node
            flake-checker
            trev.renovate
          ];
          shellHook = pkgs.trev.shellhook.ref;
        };
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

      checks = pkgs.trev.lib.mkChecks {
        node = {
          src = ./.;
          deps = with pkgs; [
            biome
          ];
          script = ''
            biome check .
          '';
        };

        nix = {
          src = ./.;
          deps = with pkgs; [
            alejandra
          ];
          script = ''
            alejandra -c .
          '';
        };

        actions = {
          src = ./.;
          deps = with pkgs; [
            prettier
            action-validator
            trev.renovate
          ];
          script = ''
            prettier --check .
            action-validator action.yaml
            action-validator .github/**/*.yaml
            renovate-config-validator .github/renovate.json
          '';
        };
      };

      formatter = pkgs.alejandra;
    });
}
