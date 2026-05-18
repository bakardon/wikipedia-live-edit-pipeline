# Download English Wikipedia Clickstream for CLICKSTREAM_MONTH (default 2026-04).
# Equivalent to `make download-clickstream` on Unix. Run from repo root or any cwd:
#   powershell -File scripts/download-clickstream.ps1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Month = if ($env:CLICKSTREAM_MONTH) { $env:CLICKSTREAM_MONTH } else { "2026-04" }
$Lang = if ($env:CLICKSTREAM_LANG) { $env:CLICKSTREAM_LANG } else { "en" }

$Rel = if ($env:CLICKSTREAM_PATH) { $env:CLICKSTREAM_PATH } else { "data/clickstream" }
$Dest = if ([System.IO.Path]::IsPathRooted($Rel)) { $Rel } else { Join-Path $Root ($Rel -replace '/', [IO.Path]::DirectorySeparatorChar) }

New-Item -ItemType Directory -Force -Path $Dest | Out-Null
$File = "clickstream-${Lang}wiki-${Month}.tsv.gz"
$Out = Join-Path $Dest $File
$Url = "https://dumps.wikimedia.org/other/clickstream/$Month/$File"

if (Test-Path $Out) {
    Write-Host "Already have $File — skipping." -ForegroundColor Yellow
    exit 0
}

Write-Host "Downloading $Url (~1.5 GB)..." -ForegroundColor Cyan
$Part = "$Out.part"
try {
    Invoke-WebRequest -Uri $Url -OutFile $Part -UseBasicParsing
    Move-Item -Force $Part $Out
} catch {
    if (Test-Path $Part) { Remove-Item -Force $Part }
    throw
}

Write-Host "Saved to $Out" -ForegroundColor Green
Get-Item $Out | Select-Object FullName, Length
