{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      flake-parts,
      process-compose-flake,
      treefmt-nix,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      imports = [
        process-compose-flake.flakeModule
        treefmt-nix.flakeModule
      ];

      perSystem =
        { pkgs, ... }:
        let
          sqlFormatterConfig = pkgs.writeText "sql-formatter.json" (
            builtins.toJSON {
              language = "postgresql";
              paramTypes.named = [ "@" ];
            }
          );
          sqlFormatter = pkgs.writeShellScriptBin "sql-formatter" ''
            exec ${pkgs.sql-formatter}/bin/sql-formatter --config ${sqlFormatterConfig} "$@"
          '';
        in
        {
          treefmt = {
            projectRootFile = "flake.nix";
            settings.excludes = [
              "pnpm-lock.yaml"
            ];
            programs.buf.enable = true;
            programs.nixfmt.enable = true;
            programs.gofumpt.enable = true;
            programs.oxfmt.enable = true;
            programs.ruff-format.enable = true;
            programs.sql-formatter = {
              enable = true;
              package = sqlFormatter;
              dialect = "postgresql";
            };
          };

          devShells.default = pkgs.mkShell {
            shellHook = ''
              export DATABASE_URL="postgres://$USER@127.0.0.1:5432/postgres?sslmode=disable"
            '';
            packages = with pkgs; [
              nodejs_26
              pnpm
              go_1_27
              just
              process-compose
              postgresql
            ];
          };

          process-compose.default = {
            cli.options.no-server = true;
            settings.processes = {
              generate = {
                command = "just generate";
              };

              pgsql = {
                command = ''
                  data_dir="$(pwd)/.data/postgres"
                  socket_dir="$data_dir/socket"
                  if [ ! -f "$data_dir/PG_VERSION" ]; then
                    mkdir -p "$data_dir"
                    ${pkgs.postgresql}/bin/initdb --auth=trust --no-locale -D "$data_dir"
                  fi
                  mkdir -p "$socket_dir"
                  exec ${pkgs.postgresql}/bin/postgres -D "$data_dir" -k "$socket_dir" -p 5432
                '';
                readiness_probe.exec.command = "${pkgs.postgresql}/bin/pg_isready -h 127.0.0.1 -p 5432 -d postgres";
              };

              rustfs = {
                command = ''
                  data_dir="$(pwd)/.data/rustfs"
                  mkdir -p "$data_dir"
                  exec ${pkgs.rustfs}/bin/rustfs server "$data_dir" --address 127.0.0.1:9000 --console-enable --console-address 127.0.0.1:9001
                '';
                environment = {
                  RUSTFS_ACCESS_KEY = "lutra";
                  RUSTFS_SECRET_KEY = "lutra-secret";
                };
                readiness_probe.exec.command = "${pkgs.curl}/bin/curl -fsS http://127.0.0.1:9000/health/ready";
              };

              api = {
                command = "go run ./cmd/server";
                depends_on.generate.condition = "process_completed_successfully";
                depends_on.pgsql.condition = "process_healthy";
                depends_on.rustfs.condition = "process_healthy";
                environment = {
                  AWS_ENDPOINT_URL_S3 = "http://127.0.0.1:9000";
                  AWS_S3_BUCKET = "lutra";
                  AWS_ACCESS_KEY_ID = "lutra";
                  AWS_SECRET_ACCESS_KEY = "lutra-secret";
                  AWS_REGION = "us-east-1";
                  AWS_S3_SECURE = "false";
                };
              };
              ui = {
                command = "pnpm --filter @lutra/console dev";
                depends_on.generate.condition = "process_completed_successfully";
              };
            };
          };
        };
    };
}
