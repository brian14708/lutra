{ lib, stdenvNoCC, fetchurl }:
let
  release = {
    x86_64-linux = {
      target = "x86_64-unknown-linux-musl";
      sha256 = "5429b216b68f3fa52b7f0e6627a2c2a86fc0c75d19b1e152cd2e7dcd8bab4c82";
    };
    aarch64-linux = {
      target = "aarch64-unknown-linux-musl";
      sha256 = "c0218d5ae5052cc2c6f842bbcfa57fd10240cfe30795c381850c452fbcef2b61";
    };
    aarch64-darwin = {
      target = "aarch64-apple-darwin";
      sha256 = "503c9a74eaad9b3579a373a5cd558b6910b179d17d87c2a75307cd975d1a299d";
    };
  }.${stdenvNoCC.hostPlatform.system};
in
stdenvNoCC.mkDerivation rec {
  pname = "restate-server";
  version = "1.7.13";

  src = fetchurl {
    url = "https://github.com/restatedev/restate/releases/download/v${version}/restate-server-${release.target}.tar.xz";
    inherit (release) sha256;
  };

  dontConfigure = true;
  dontBuild = true;
  dontFixup = true;

  installPhase = ''
    runHook preInstall
    install -Dm755 restate-server "$out/bin/restate-server"
    runHook postInstall
  '';

  meta = {
    description = "Durable execution server";
    homepage = "https://restate.dev";
    mainProgram = "restate-server";
    platforms = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
    sourceProvenance = [ lib.sourceTypes.binaryNativeCode ];
  };
}
