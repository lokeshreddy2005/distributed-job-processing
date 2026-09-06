# Shared paths/ports for all orchestration scripts. Dot-source this file.
$Root       = Split-Path -Parent $PSScriptRoot
$RedisDir   = Join-Path $Root "infra\redis"
$PgDir      = Join-Path $Root "infra\postgres"
$PromDir    = Join-Path $Root "infra\prometheus"
$RunDir     = Join-Path $Root "run"
$LogDir     = Join-Path $Root "logs"
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"

$RedisPort  = 16379
$PgPort     = 15432
$ApiPort    = 8000
$PromPort   = 9090
$WorkerMetricsBasePort = 9101

New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Save-Pid([string]$Name, [int]$ProcId) {
    $pidFile = Join-Path $RunDir "$Name.pid"
    Set-Content -Path $pidFile -Value $ProcId -Encoding utf8
}

function Get-SavedPid([string]$Name) {
    # Returns the pid only if it's still a live process. A pid file left
    # behind by an unclean shutdown (machine sleep, crash, killed outside
    # stop_all.ps1) would otherwise make start_all.ps1 wrongly believe that
    # process is "already running" and skip starting it - this was found
    # for real during this project's own verification pass.
    $pidFile = Join-Path $RunDir "$Name.pid"
    if (Test-Path $pidFile) {
        $procId = [int](Get-Content $pidFile)
        if (Get-Process -Id $procId -ErrorAction SilentlyContinue) {
            return $procId
        }
        Remove-Item $pidFile -ErrorAction SilentlyContinue
    }
    return $null
}

function Stop-SavedProcess([string]$Name, [switch]$Force) {
    $procId = Get-SavedPid $Name
    if ($null -ne $procId) {
        $p = Get-Process -Id $procId -ErrorAction SilentlyContinue
        if ($p) {
            if ($Force) { Stop-Process -Id $procId -Force }
            else { Stop-Process -Id $procId -Force }
            Write-Host "Stopped $Name (pid $procId)"
        }
        Remove-Item (Join-Path $RunDir "$Name.pid") -ErrorAction SilentlyContinue
    }
}
