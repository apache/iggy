{
  description = "iggy-rs";
  inputs = {
    nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";
    systems.url = "github:nix-systems/default";
    flake-parts.url = "github:hercules-ci/flake-parts";
    devshell.url = "github:numtide/devshell";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-flake = {
      url = "github:juspay/rust-flake";
      inputs.rust-overlay.follows = "rust-overlay";
    };
  };

  outputs = inputs@{ nixpkgs, rust-overlay, ... }:
    inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      systems = import inputs.systems;

      imports = [
        inputs.rust-flake.flakeModules.default
        inputs.rust-flake.flakeModules.nixpkgs
      ];

      flake = {
        nix-health.default = {
          nix-version.min-required = "2.16.0";
          direnv.required = true;
        };
      };

      perSystem = { config, self', pkgs, lib, system, ... }: {
        devShells.default = pkgs.mkShell {
          inputsFrom = [
            self'.devShells.rust
	  ];
	};
	packages.default = config.packages."server";
        rust-project = let extraArgs = {
          nativeBuildInputs = with pkgs; [
	    perl
            pkg-config
            stdenv.cc
          ] ++ lib.optionals stdenv.buildPlatform.isDarwin [
            libiconv
          ];
	}; in {
	  src = ./.;
	  toolchain = pkgs.rust-bin.stable.latest.default;
	  crates.server.crane.args = extraArgs;
	  crates.integration.crane.args = extraArgs;
	};
      };
    };
}
