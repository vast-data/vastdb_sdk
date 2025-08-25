{ local ? import <nixpkgs> { } }:

let
  pkgs = import (fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/tarball/nixpkgs-unstable";
  }) { };
in pkgs.mkShellNoCC {
  LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";

  packages = (with pkgs; [
    stdenv.cc.cc.lib

    openssl
    pkg-config

    pyright
    ruff

    python312
    python312Packages.autopep8
    python312Packages.flatbuffers
  ]);
}

