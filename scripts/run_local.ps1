param(
    [string]$HostAddress = "0.0.0.0",
    [int]$Port = 5000,
    [switch]$NoRun
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

if (-not $env:FLASK_SECRET_KEY) {
    $env:FLASK_SECRET_KEY = "dev-local-secret"
}

$env:DATABASE_BACKEND = "sqlite"
$env:STRICT_ONLINE_DATABASE = "false"
$env:SESSION_COOKIE_SECURE = "0"
$env:CSRF_PROTECTION = "1"

Write-Host "Ambiente local:" -ForegroundColor Cyan
Write-Host "  DATABASE_BACKEND=$env:DATABASE_BACKEND"
Write-Host "  STRICT_ONLINE_DATABASE=$env:STRICT_ONLINE_DATABASE"
Write-Host "  SESSION_COOKIE_SECURE=$env:SESSION_COOKIE_SECURE"
Write-Host "  CSRF_PROTECTION=$env:CSRF_PROTECTION"
Write-Host ""
Write-Host "Abrir no navegador:" -ForegroundColor Cyan
Write-Host "  http://127.0.0.1:$Port/login"
try {
    $localIp = Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object { $_.IPAddress -notlike "127.*" -and $_.IPAddress -notlike "169.254.*" } |
        Select-Object -First 1 -ExpandProperty IPAddress
    if ($localIp) {
        Write-Host "  http://$localIp`:$Port/login"
    }
} catch {
}
Write-Host ""

if ($NoRun) {
    Write-Host "Validacao concluida; servidor nao iniciado por causa de -NoRun." -ForegroundColor Yellow
    exit 0
}

python -m flask --app app run --host $HostAddress --port $Port
