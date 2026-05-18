# One-time (or repeat) Clickstream path: download dump -> Spark batch -> dbt models.
# Requires: Docker stack with Postgres up (e.g. scripts/stack-up.ps1), ~1.5 GB disk, dbt on PATH.
#   powershell -File scripts/setup-clickstream.ps1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "=== 1/3 Download Clickstream dump ===" -ForegroundColor Cyan
& "$PSScriptRoot\download-clickstream.ps1"

Write-Host ""
Write-Host "=== 2/3 Spark batch -> raw.stg_clickstream ===" -ForegroundColor Cyan
$pgUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "wiki" }
$pgDb = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "wiki" }
docker compose exec -T postgres pg_isready -U $pgUser -d $pgDb | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Postgres is not reachable. Start the stack first, e.g.:" -ForegroundColor Yellow
    Write-Host "  powershell -File .\scripts\stack-up.ps1" -ForegroundColor Yellow
    exit 1
}

docker compose --profile batch run --rm batch
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "=== 3/3 dbt -> core.fact_clickstream + marts ===" -ForegroundColor Cyan
$dbtCmd = Get-Command dbt -ErrorAction SilentlyContinue
if (-not $dbtCmd) {
    Write-Host "dbt is not on PATH. Install with: pip install dbt-postgres" -ForegroundColor Red
    exit 1
}
& "$PSScriptRoot\run-dbt.ps1"
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Done. Refresh the dashboard (Stream vs Batch tab)." -ForegroundColor Green
