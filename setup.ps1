# ============================================================
# VendorGlobe_API one-shot setup -- run in ADMIN PowerShell on
# the app server. Re-runnable: safe to execute again after edits.
#
# Prereqs on the server:
#   - Python 3.11+ on PATH (or edit $Py below)
#   - Git
#   - ODBC Driver 17 or 18 for SQL Server
#   - NSSM (edit $Nssm below, or put nssm.exe next to this script)
# ============================================================

$ErrorActionPreference = "Stop"

# ---- EDIT THESE THREE IF YOUR PATHS DIFFER ----
$Root = "D:\VendorGlobe_API"                 # where the repo lives
$Py   = "python"                              # or full path to python.exe
$Nssm = "$Root\nssm.exe"                      # or full path to nssm.exe

$Svc  = "VendorGlobeAPI"
$App  = "$Root\app"

Write-Host "== 1/5 Installing Python dependencies =="
& $Py -m pip install -r "$Root\requirements.txt"

Write-Host "== 2/5 Registering Windows service ($Svc) via NSSM =="
& $Nssm status $Svc 2>$null
if ($LASTEXITCODE -ne 0) {
    & $Nssm install $Svc (Get-Command $Py).Source "$App\app.py"
} else {
    Write-Host "   service exists - updating settings"
}
& $Nssm set $Svc AppDirectory $App
& $Nssm set $Svc AppStdout "$App\service_stdout.log"
& $Nssm set $Svc AppStderr "$App\service_stderr.log"
& $Nssm set $Svc AppEnvironmentExtra "PYTHONUNBUFFERED=1"
& $Nssm set $Svc AppThrottle 5000
& $Nssm set $Svc AppExit Default Restart

Write-Host "== 3/5 Firewall rule (port 5002, SAP + internal subnets) =="
Remove-NetFirewallRule -DisplayName "VendorGlobe API" -ErrorAction SilentlyContinue
New-NetFirewallRule -DisplayName "VendorGlobe API" -Direction Inbound -Action Allow `
    -Protocol TCP -LocalPort 5002 `
    -RemoteAddress 192.168.26.0/24, 192.168.66.0/24, 192.168.10.0/24 | Out-Null

Write-Host "== 4/5 Starting service =="
Restart-Service $Svc -ErrorAction SilentlyContinue
if ((Get-Service $Svc).Status -ne "Running") { Start-Service $Svc }
Start-Sleep 8

Write-Host "== 5/5 Health check =="
Get-Service $Svc
try {
    $h = Invoke-RestMethod "http://localhost:5002/health" -TimeoutSec 10
    Write-Host "HEALTH:" ($h | ConvertTo-Json -Compress)
} catch {
    Write-Host "Health check failed - see $App\service_stdout.log" -ForegroundColor Yellow
    Get-Content "$App\service_stdout.log" -Tail 10 -ErrorAction SilentlyContinue
}
Write-Host "Done. Logs: $App\service_stdout.log and access.log"
