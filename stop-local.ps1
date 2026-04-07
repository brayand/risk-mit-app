$ErrorActionPreference = "Continue"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$pidFile = Join-Path $root ".local-dev-pids.json"
$backendPidFile = Join-Path $root ".backend-dev.pid"
$frontendPidFile = Join-Path $root ".frontend-dev.pid"

function Stop-IfRunning([int]$pid) {
  if ($pid -le 0) { return }
  $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
  if ($null -ne $proc) {
    Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
    Write-Host "Stopped process PID $pid"
  }
}

if (Test-Path $pidFile) {
  try {
    $data = Get-Content -Path $pidFile -Raw | ConvertFrom-Json
    Stop-IfRunning -pid ([int]$data.backendPid)
    Stop-IfRunning -pid ([int]$data.frontendPid)
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
  } catch {
    Write-Warning "Could not parse PID file. Falling back to port-based stop."
  }
}

if (Test-Path $backendPidFile) {
  $backendPidRaw = Get-Content -Path $backendPidFile -Raw -ErrorAction SilentlyContinue
  if ($backendPidRaw) { Stop-IfRunning -pid ([int]$backendPidRaw.Trim()) }
  Remove-Item $backendPidFile -Force -ErrorAction SilentlyContinue
}

if (Test-Path $frontendPidFile) {
  $frontendPidRaw = Get-Content -Path $frontendPidFile -Raw -ErrorAction SilentlyContinue
  if ($frontendPidRaw) { Stop-IfRunning -pid ([int]$frontendPidRaw.Trim()) }
  Remove-Item $frontendPidFile -Force -ErrorAction SilentlyContinue
}

# Fallback: kill anything listening on app ports.
$ports = @(8000, 3000)
foreach ($port in $ports) {
  $listeners = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
  if ($listeners) {
    $listeners |
      Select-Object -ExpandProperty OwningProcess -Unique |
      ForEach-Object {
        Stop-IfRunning -pid ([int]$_)
      }
  }
}

Write-Host "Stop complete."
