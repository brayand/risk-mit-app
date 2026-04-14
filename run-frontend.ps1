$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$frontendPath = Join-Path $root "frontend"
$pidFile = Join-Path $root ".frontend-dev.pid"
$frontendCmd = @"
if (!(Test-Path "node_modules")) { npm install }
npm run dev
"@

$frontendProc = Start-Process powershell -WorkingDirectory $frontendPath -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $frontendCmd -PassThru

$frontendProc.Id | Set-Content -Path $pidFile -Encoding UTF8
Write-Host "Frontend starting at http://localhost:3000 (PID $($frontendProc.Id))"
Write-Host "PID file written to $pidFile"
