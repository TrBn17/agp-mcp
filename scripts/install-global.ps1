param(
    [switch]$Uninstall,
    [switch]$PipInstall
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$SourcePlugin = Join-Path $ProjectRoot "plugins\agp-mesh"
$GlobalPluginsDir = Join-Path $env:USERPROFILE ".gemini\config\plugins"
$TargetDir = Join-Path $GlobalPluginsDir "agp-mesh"
$NormalizedRoot = $ProjectRoot -replace '\\', '/'
$StdioEntry = "$NormalizedRoot/plugins/agp-mesh/scripts/agp_mcp_stdio.py"

if ($Uninstall) {
    try { agy plugin disable agp-mesh 2>$null | Out-Null } catch {}
    try { agy mcp remove agp-broker 2>$null | Out-Null } catch {}
    if (Test-Path $TargetDir) {
        $item = Get-Item $TargetDir -Force
        if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
            cmd /c rmdir "$TargetDir" | Out-Null
        } else {
            Remove-Item -Recurse -Force $TargetDir
        }
        Write-Host "[AGP] Uninstalled global plugin: $TargetDir" -ForegroundColor Yellow
    } else {
        Write-Host "[AGP] Global plugin not found." -ForegroundColor Cyan
    }
    exit 0
}

if (-not (Test-Path $SourcePlugin)) {
    throw "Source plugin directory not found: $SourcePlugin"
}

if ($PipInstall) {
    Write-Host "[AGP] Installing standard Python MCP package (agp-mcp, editable)..." -ForegroundColor Cyan
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & python -m pip install --disable-pip-version-check --no-build-isolation --no-deps -e $ProjectRoot 2>&1 | Out-Host
    $pipExit = $LASTEXITCODE
    $ErrorActionPreference = $prevEap
    if ($pipExit -ne 0) {
        throw "pip install failed with exit code $pipExit"
    }
}

New-Item -ItemType Directory -Path $GlobalPluginsDir -Force | Out-Null

# Keep source mcp_config.json updated using UTF-8 WITHOUT BOM (Go JSON parser rejects BOM!)
$Utf8NoBom = [System.Text.UTF8Encoding]::new($false)
$McpCfgPath = Join-Path $SourcePlugin "mcp_config.json"
$McpJson = @"
{
  "mcpServers": {
    "agp-broker": {
      "command": "python",
      "args": [
        "$StdioEntry"
      ],
      "env": {
        "AGP_HOME": "$NormalizedRoot"
      }
    }
  }
}
"@
[System.IO.File]::WriteAllText($McpCfgPath, $McpJson + "`n", $Utf8NoBom)

# Copy as a real directory (Antigravity's Go hook loader ignores Windows directory junctions)
if (Test-Path $TargetDir) {
    $item = Get-Item $TargetDir -Force
    if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
        cmd /c rmdir "$TargetDir" | Out-Null
    } else {
        Remove-Item -Recurse -Force $TargetDir
    }
}
Copy-Item -Recurse -Force $SourcePlugin $TargetDir
# Register MCP server once globally via agy mcp add so it doesn't duplicate with plugin mcp_config.json
Remove-Item -Force (Join-Path $TargetDir "mcp_config.json") -ErrorAction SilentlyContinue

agy plugin enable agp-mesh 2>&1 | Out-Host
try { agy mcp remove agp-broker 2>$null | Out-Null } catch {}
agy mcp add -e "AGP_HOME=$NormalizedRoot" agp-broker python "$StdioEntry" 2>&1 | Out-Host

Write-Host "[AGP] Installed Standard Serverless MCP Suite (v2.1.0):" -ForegroundColor Green
Write-Host "      Plugin : $TargetDir (Hooks: SessionStart, PreInvocation, PostInvocation, PreToolUse, PostToolUse, Stop)" -ForegroundColor Cyan
Write-Host "      MCP    : agp-broker (serverless stdio -> $StdioEntry)" -ForegroundColor Cyan
Write-Host "      State  : $env:USERPROFILE\.gemini\agp\ (SQLite WAL + cross-process locked Teams)" -ForegroundColor Cyan
Write-Host "[AGP] AI sessions now operate SILENTLY in the background with ZERO HTTP server needed!" -ForegroundColor Green
Write-Host "[AGP] Optional: run 'scripts\open-monitor.ps1' ONLY when you want to view the web UI." -ForegroundColor DarkGray
