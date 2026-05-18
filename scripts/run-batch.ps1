# One-shot Clickstream Spark batch (same as `make batch`). Requires ./data/clickstream with the .tsv.gz file.
#   powershell -File scripts/run-batch.ps1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

docker compose --profile batch run --rm batch
