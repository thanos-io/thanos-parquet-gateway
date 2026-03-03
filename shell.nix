let
  pkgs = import <nixpkgs> { };
in
pkgs.mkShell {
  name = "env";
  hardeningDisable = [ "fortify" ];
  buildInputs = with pkgs; [
    go
    gotools
    delve
    revive
    duckdb
    protobuf
    protoc-gen-go
    golangci-lint
  ];
  shellHook = ''
    export PATH="$(go env GOPATH)/bin:$PATH"
    export GOPATH="$(go env GOPATH)"
  '';
}

