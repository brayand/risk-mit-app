$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendRunner = Join-Path $root "run-backend.ps1"
$frontendRunner = Join-Path $root "run-frontend.ps1"

if (!(Test-Path $backendRunner)) { throw "Missing script: $backendRunner" }
if (!(Test-Path $frontendRunner)) { throw "Missing script: $frontendRunner" }

& $backendRunner
Start-Sleep -Seconds 2
& $frontendRunner

Write-Host "Local environment start complete."
