{
  description = "Nix Simple Cache Action";

  nixConfig = {
    extra-substituters = [
      "https://cache.trev.zip/nur"
    ];
    extra-trusted-public-keys = [
      "nur:70xGHUW1+1b8FqBchldaunN//pZNVo6FKuPL4U/n844="
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
        overlays = [
          nur.overlays.packages
          nur.overlays.libs
        ];
      };

      node = pkgs.nodejs_24; # increment as needed
    in {
      devShells = {
        default = pkgs.mkShell {
          packages = with pkgs; [
            node
            biome
            prettier
            alejandra
          ];
          shellHook = pkgs.shellhook.ref;
        };

        ci = pkgs.mkShell {
          packages = with pkgs; [
            node
            biome
            flake-checker
            renovate
          ];
        };
      };

      packages.default = pkgs.buildNpmPackage (finalAttrs: {
        pname = "nix-simple-cache-action";
        version = "1.5.2";
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

      checks = pkgs.lib.mkChecks {
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
            renovate
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
