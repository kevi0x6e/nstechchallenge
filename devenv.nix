{ pkgs, ... }:

# Every tool the repo needs, pinned through devenv.lock. Host prerequisites: Docker + Nix + devenv.
{
  packages = with pkgs; [
    just
    kind
    kubectl
    devspace
    jq
  ];

  languages.dotnet = {
    enable = true;
    package = pkgs.dotnetCorePackages.sdk_10_0;
  };

  enterShell = ''
    echo "fleet: 'just up' to start the environment, 'just e2e' to prove it."
  '';
}
