# Run dbt models against Postgres exposed on localhost:5544 (same as `make dbt`).
# Install dbt first: pip install dbt-postgres
#   powershell -File scripts/run-dbt.ps1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location (Join-Path $Root "dbt")

$env:POSTGRES_HOST = if ($env:POSTGRES_HOST) { $env:POSTGRES_HOST } else { "localhost" }
$env:POSTGRES_PORT = if ($env:POSTGRES_PORT) { $env:POSTGRES_PORT } else { "5544" }

dbt run --profiles-dir .
