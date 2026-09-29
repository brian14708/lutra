{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    process-compose-flake.url = "github:Platonic-Systems/process-compose-flake";
  };

  outputs =
    inputs@{
      flake-parts,
      process-compose-flake,
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
      ];

      perSystem =
        { pkgs, config, ... }:
        {
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

          process-compose.dev = {
            cli.options.no-server = true;
            settings.processes = {
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
            };
          };

          process-compose.integration =
            let
              dev = config.process-compose.dev.settings.processes;
            in
            {
              cli.options.no-server = true;
              settings.environment.DATABASE_URL = "postgres://127.0.0.1:5432/postgres?sslmode=disable";
              settings.processes = {
                build.command = "just build";
                pgsql = dev.pgsql;
                rustfs = dev.rustfs;
                server = {
                  command = "go run ./cmd/server";
                  depends_on = {
                    build.condition = "process_completed_successfully";
                    pgsql.condition = "process_healthy";
                    rustfs.condition = "process_healthy";
                  };
                  environment = {
                    AWS_ENDPOINT_URL_S3 = "http://127.0.0.1:9000";
                    AWS_S3_BUCKET = "lutra";
                    AWS_ACCESS_KEY_ID = "lutra";
                    AWS_SECRET_ACCESS_KEY = "lutra-secret";
                    AWS_REGION = "us-east-1";
                    AWS_S3_SECURE = "false";
                  };
                };
              };
            };
        };
    };
}
