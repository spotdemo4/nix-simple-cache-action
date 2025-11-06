# Nix Simple Cache Action

![check](https://github.com/spotdemo4/nix-simple-cache-action/actions/workflows/check.yaml/badge.svg)
![vulnerable](https://github.com/spotdemo4/nix-simple-cache-action/actions/workflows/vulnerable.yaml/badge.svg)

This action saves and restores the nix store to/from the actions cache. It is mainly useful for Gitea & Forgejo, as they are unsupported by the other nix cache actions. 

## Requirements

Any action that installs Nix 
- [DeterminateSystems/nix-installer-action](https://github.com/DeterminateSystems/nix-installer-action)
- [cachix/install-nix-action](https://github.com/cachix/install-nix-action)
- etc

If you're using Gitea/Forgejo actions, make sure the runner has Node.js >= 20

## Inputs

### `max-size`

Maximum size of the cache in bytes. If the cache grows greater than this amount, it will be re-created with only the latest run's store. Defaults to 1GB.

## Outputs

### `cache-hit`

A string value to indicate if the cache exists. `"true"` or `"false"`.

## Example usage

Gitea/Forgejo:

```yaml
uses: https://github.com/DeterminateSystems/nix-installer-action@main
uses: https://github.com/spotdemo4/nix-simple-cache-action@v1.5.1
```

GitHub:

```yaml
uses: DeterminateSystems/nix-installer-action@main
uses: spotdemo4/nix-simple-cache-action@v1.5.1
```

## Alternatives
- [DeterminateSystems/magic-nix-cache-action](https://github.com/DeterminateSystems/magic-nix-cache-action)
- [nix-community/cache-nix-action](https://github.com/nix-community/cache-nix-action)
- [cachix/cachix-action](https://github.com/cachix/cachix-action)