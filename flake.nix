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
        { pkgs, ... }:
        let
          devEnvironment = {
            AWS_ENDPOINT_URL_S3 = "http://127.0.0.1:9000";
            AWS_S3_BUCKET = "lutra";
            AWS_ACCESS_KEY_ID = "lutra";
            AWS_SECRET_ACCESS_KEY = "lutra-secret";
            AWS_REGION = "us-east-1";
            AWS_S3_SECURE = "false";
            DATABASE_URL = "postgres://127.0.0.1:5432/postgres?sslmode=disable";
            LUTRA_BOOTSTRAP_API_KEY = "test";
            LUTRA_JWT_KEY = "lutra-development-jwt-key-please-do-not-use-in-production";
            LUTRA_CONSOLE_DIR = "console/dist/client";
            LUTRA_WORKER_ENDPOINT = "http://127.0.0.1:8080/worker-api";
          };
          processComposeBase = {
            cli.options.no-server = true;
            settings = {
              environment = devEnvironment;
              processes = {
                db = {
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

                s3 = {
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
          };
          nginxConfig = pkgs.writers.writeNginxConfig "lutra-nginx.conf" ''
            pid /tmp/lutra-nginx.pid;
            error_log stderr info;
            events {}
            http {
              access_log /dev/stdout;
              upstream lutra_servers {
                server 127.0.0.1:18080;
                server 127.0.0.1:18081;
                server 127.0.0.1:18082;
                keepalive 32;
              }
              server {
                listen 0.0.0.0:8080;
                listen [::]:8080;
                location / {
                  proxy_pass http://lutra_servers;
                  proxy_http_version 1.1;
                  proxy_set_header Host $host;
                  proxy_set_header X-Real-IP $remote_addr;
                  proxy_set_header Connection "";
                }
              }
            }
          '';
        in
        {
          devShells.default = pkgs.mkShell {
            env = devEnvironment;
            packages = with pkgs; [
              nodejs_26
              pnpm
              go_1_27
              just
              postgresql
              uv
            ];
          };

          process-compose.dev = processComposeBase;

          process-compose.integration = processComposeBase // {
            settings.processes = processComposeBase.settings.processes // {
              build = {
                command = "just build";
              };
              start = {
                command = ''
                  export LUTRA_WORKER_ENDPOINT="http://127.0.0.1:$((18080 + PC_REPLICA_NUM))/worker-api"
                  exec go run ./cmd/server --addr ":$((18080 + PC_REPLICA_NUM))"
                '';
                replicas = 3;
                depends_on.build.condition = "process_completed_successfully";
                depends_on.db.condition = "process_healthy";
                depends_on.s3.condition = "process_healthy";
              };
              nginx = {
                command = "exec ${pkgs.nginxMainline}/bin/nginx -e stderr -c ${nginxConfig} -g 'daemon off;'";
              };
            };
          };
        };
    };
}
