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
          containerRuntime = if pkgs.stdenv.hostPlatform.isLinux then "podman" else "docker";
          restateServer = pkgs.callPackage ./nix/restate-server.nix { };
          restateEnvironment = {
            LUTRA_RESTATE_INGRESS = "http://127.0.0.1:8081";
            LUTRA_RESTATE_ADMIN = "http://127.0.0.1:9070";
            LUTRA_RESTATE_CALLBACK = "http://127.0.0.1:8080/durable";
          };
          corsFile = pkgs.writeText "lutra-blob-cors.xml" ''
            <CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <CORSRule>
                <AllowedOrigin>*</AllowedOrigin>
                <AllowedMethod>GET</AllowedMethod>
                <AllowedMethod>HEAD</AllowedMethod>
                <AllowedMethod>PUT</AllowedMethod>
                <AllowedHeader>*</AllowedHeader>
                <ExposeHeader>ETag</ExposeHeader>
                <MaxAgeSeconds>3600</MaxAgeSeconds>
              </CORSRule>
            </CORSConfiguration>
          '';
          commonProcesses = {
            restate = {
              command = ''
                export RESTATE_BASE_DIR="$(pwd)/.data/restate"
                exec ${restateServer}/bin/restate-server --no-logo --listen-mode=tcp
              '';
              environment = {
                RESTATE_NODE_NAME = "lutra";
                RESTATE_CLUSTER_NAME = "lutra";
                RESTATE_BIND_ADDRESS = "127.0.0.1:5122";
                RESTATE_ADVERTISED_ADDRESS = "http://127.0.0.1:5122";
                RESTATE_ADMIN__BIND_ADDRESS = "127.0.0.1:9070";
                RESTATE_INGRESS__BIND_ADDRESS = "127.0.0.1:8081";
              };
              readiness_probe.exec.command = "${pkgs.curl}/bin/curl -fsS http://127.0.0.1:9070/health";
            };

            pgsql = {
              command = ''
                data_dir="$(pwd)/.data/postgres"
                if [ ! -f "$data_dir/PG_VERSION" ]; then
                  mkdir -p "$data_dir"
                  ${pkgs.postgresql}/bin/initdb --auth=trust --no-locale -D "$data_dir"
                fi
                exec ${pkgs.postgresql}/bin/postgres -D "$data_dir" -k "" -p 5432
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

            blob-init = {
              command = ''
                export MC_CONFIG_DIR="$(pwd)/.data/mc"
                ${pkgs.minio-client}/bin/mc alias set lutra "$AWS_ENDPOINT_URL_S3" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" --path on
                ${pkgs.minio-client}/bin/mc mb --ignore-existing "lutra/$AWS_S3_BUCKET"
                ${pkgs.minio-client}/bin/mc cors set "lutra/$AWS_S3_BUCKET" ${corsFile}
              '';
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
          };
        in
        {
          packages.restate-server = restateServer;

          devShells.default = pkgs.mkShell (restateEnvironment // {
            shellHook = ''
              export DATABASE_URL="postgres://$USER@127.0.0.1:5432/postgres?sslmode=disable"
              export LUTRA_URL="http://127.0.0.1:8080/api"
              export LUTRA_CONTAINER_RUNTIME="''${LUTRA_CONTAINER_RUNTIME:-${containerRuntime}}"
            '';
            packages = (with pkgs; [
              nodejs_26
              pnpm
              go_1_27
              uv
              just
              process-compose
              postgresql
              rustfs
              minio-client
              curl
              docker-client
            ]) ++ [ restateServer ] ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [ pkgs.podman ];
          });

          process-compose.dev = {
            cli.options.no-server = true;
            settings.processes = commonProcesses;
          };

          process-compose.integration = {
            cli.options.no-server = true;
            settings.environment.DATABASE_URL = "postgres://127.0.0.1:5432/postgres?sslmode=disable";
            settings.processes = commonProcesses // {
              build.command = "just build";
              server = {
                command = ''
                  export LUTRA_CONTAINER_RUNTIME="''${LUTRA_CONTAINER_RUNTIME:-${containerRuntime}}"
                  exec go run ./cmd/server
                '';
                depends_on = {
                  build.condition = "process_completed_successfully";
                  pgsql.condition = "process_healthy";
                  blob-init.condition = "process_completed_successfully";
                  restate.condition = "process_healthy";
                };
                environment = commonProcesses.blob-init.environment // restateEnvironment;
              };
            };
          };
        };
    };
}
