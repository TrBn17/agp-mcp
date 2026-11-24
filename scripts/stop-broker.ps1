param(
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 47821
)

$ErrorActionPreference = "SilentlyContinue"
$PidFile = Join-Path $env:USERPROFILE ".gemini\agp\broker.pid"
$Stopped = $false

try {
    $health = Invoke-RestMethod -Uri "http://${BindHost}:${Port}/api/health" -TimeoutSec 2
    if ($health.pid) {
        Stop-Process -Id ([int]$health.pid) -Force
        Write-Host "[AGY Monitor] Closed optional Monitor UI PID $($health.pid)." -ForegroundColor Yellow
        $Stopped = $true
    }
} catch {}

if (-not $Stopped -and (Test-Path $PidFile)) {
    $savedPid = (Get-Content $PidFile -Raw).Trim()
    if ($savedPid -match '^\d+$') {
        Stop-Process -Id ([int]$savedPid) -Force
        Write-Host "[AGY Monitor] Closed Monitor UI PID $savedPid from pidfile." -ForegroundColor Yellow
        $Stopped = $true
    }
    Remove-Item $PidFile -Force
}

# Sweep any stray/duplicate agp.broker Monitor UI processes bound to the same port
$strays = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -like 'python*' -and $_.CommandLine -like '*agp.broker*' -and ($_.CommandLine -like "*$Port*" -or $Port -eq 47821)
}
foreach ($p in $strays) {
    Stop-Process -Id ([int]$p.ProcessId) -Force
    Write-Host "[AGY Monitor] Closed stray Monitor UI PID $($p.ProcessId)." -ForegroundColor Yellow
    $Stopped = $true
}
if (Test-Path $PidFile) { Remove-Item $PidFile -Force }

if ($Stopped) {
    Write-Host "[AGY Monitor] AI MCP tools, Agent Teams, and hooks continue running silently in the background." -ForegroundColor Green
} else {
    Write-Host "[AGY Monitor] No running Monitor UI detected on http://${BindHost}:${Port}." -ForegroundColor Cyan
}
