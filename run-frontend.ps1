$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$frontendPath = Join-Path $root "frontend"
$pidFile = Join-Path $root ".frontend-dev.pid"
$frontendCmd = @"
corepack enable
corepack prepare pnpm@latest --activate
if (!(Test-Path "node_modules")) { pnpm install }
pnpm dev
"@

$frontendProc = Start-Process powershell -WorkingDirectory $frontendPath -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $frontendCmd -PassThru

$frontendProc.Id | Set-Content -Path $pidFile -Encoding UTF8
Write-Host "Frontend starting at http://localhost:3000 (PID $($frontendProc.Id))"
Write-Host "PID file written to $pidFile"
