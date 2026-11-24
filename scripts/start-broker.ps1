param(
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 47821,
    [switch]$Foreground,
    [switch]$NoBrowser
)

# Alias wrapper to open-monitor.ps1 (Monitor UI is strictly optional; MCP runs serverless in background)
& (Join-Path $PSScriptRoot "open-monitor.ps1") -BindHost $BindHost -Port $Port -Foreground:$Foreground -NoBrowser:$NoBrowser
