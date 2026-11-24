param(
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 47821,
    [switch]$Foreground,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Url = "http://${BindHost}:${Port}"

Write-Host "[AGY Monitor] Opening OPTIONAL HTML Dashboard UI (AI MCP already runs silently without this)." -ForegroundColor Cyan

# Check if Monitor UI is already running
try {
    $health = Invoke-RestMethod -Uri "$Url/api/health" -TimeoutSec 2
    if ($health.ok) {
        Write-Host "[AGY Monitor] Dashboard UI already open at $Url (PID: $($health.pid))" -ForegroundColor Green
        if (-not $NoBrowser) {
            Start-Process $Url
        }
        exit 0
    }
} catch {
    # Not running yet
}

if ($Foreground) {
    Push-Location $ProjectRoot
    try {
        $pyArgs = @("-m", "agp.broker", "--host", $BindHost, "--port", "$Port")
        if (-not $NoBrowser) { $pyArgs += "--open-browser" }
        & python @pyArgs
    } finally {
        Pop-Location
    }
} else {
    $pyArgs = @("-m", "agp.broker", "--host", $BindHost, "--port", "$Port")
    if (-not $NoBrowser) { $pyArgs += "--open-browser" }
    Start-Process -FilePath "python" -ArgumentList $pyArgs -WorkingDirectory $ProjectRoot -WindowStyle Hidden
    $deadline = (Get-Date).AddSeconds(6)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 250
        try {
            $health = Invoke-RestMethod -Uri "$Url/api/health" -TimeoutSec 1
            if ($health.ok) {
                Write-Host "[AGY Monitor] Dashboard UI started at $Url (PID: $($health.pid))" -ForegroundColor Green
                Write-Host "[AGY Monitor] Closing this UI later (scripts\stop-broker.ps1) will NOT stop AI MCP communication." -ForegroundColor DarkGray
                exit 0
            }
        } catch {}
    }
    Write-Error "[AGY Monitor] Failed to become healthy within 6 seconds."
    exit 1
}
