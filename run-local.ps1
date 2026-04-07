$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendPath = Join-Path $root "backend"
$frontendPath = Join-Path $root "frontend"
$pidFile = Join-Path $root ".local-dev-pids.json"

# Backend command (creates venv if missing, installs deps, starts FastAPI)
$backendCmd = @"
Set-Location "$backendPath"
if (!(Test-Path ".venv")) { python -m venv .venv }
& ".\.venv\Scripts\Activate.ps1"
pip install -r requirements.txt
uvicorn api:app --reload --port 8000
"@

# Frontend command (installs deps if needed, starts Vite)
$frontendCmd = @"
Set-Location "$frontendPath"
if (!(Test-Path "node_modules")) { npm install }
npm run dev
"@

$backendProc = Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $backendCmd -PassThru
Start-Sleep -Seconds 2
$frontendProc = Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $frontendCmd -PassThru

$pidData = @{
  backendPid = $backendProc.Id
  frontendPid = $frontendProc.Id
  startedAt = (Get-Date).ToString("o")
}

$pidData | ConvertTo-Json | Set-Content -Path $pidFile -Encoding UTF8

Write-Host "Backend starting at http://localhost:8000 (PID $($backendProc.Id))"
Write-Host "Frontend starting at http://localhost:3000 (PID $($frontendProc.Id))"
Write-Host "PID file written to $pidFile"
