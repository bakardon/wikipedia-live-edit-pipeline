# Start Kafka, Postgres, SSE producer, Spark streaming, and Streamlit (same as `make up`).
# Run from Windows Terminal (PowerShell 5.1):  powershell -File scripts/stack-up.ps1
# Or from an open PowerShell prompt in the repo root:  .\scripts\stack-up.ps1
# Prerequisite: Docker Desktop running; execute from the repository root.

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example" -ForegroundColor Yellow
}

docker compose up -d --build kafka postgres producer streaming dashboard
Write-Host ""
Write-Host "Dashboard: http://localhost:8501" -ForegroundColor Green
Write-Host "Postgres on host: localhost:5544 (user/db: wiki / wiki unless overridden in .env)" -ForegroundColor Green
