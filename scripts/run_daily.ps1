param(
  [ValidateSet("US", "JP", "ALL", "REVIEW")]
  [string]$Mode = "ALL"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

if ($Mode -eq "REVIEW") {
  python -m market_sim.cli review
} elseif ($Mode -eq "ALL") {
  python -m market_sim.cli run --markets US JP
} else {
  python -m market_sim.cli run --markets $Mode
}
