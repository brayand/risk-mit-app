$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendPath = Join-Path $root "backend"
$pidFile = Join-Path $root ".backend-dev.pid"

$backendCmd = @"
if (!(Test-Path ".venv")) { python -m venv .venv }
& ".\.venv\Scripts\python.exe" -m pip install -r ".\requirements.txt"
& ".\.venv\Scripts\python.exe" -m uvicorn api:app --reload --port 8000
"@

$backendProc = Start-Process powershell -WorkingDirectory $backendPath -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $backendCmd -PassThru

$backendProc.Id | Set-Content -Path $pidFile -Encoding UTF8
Write-Host "Backend starting at http://localhost:8000 (PID $($backendProc.Id))"
Write-Host "PID file written to $pidFile"
