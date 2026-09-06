# Starts the full local-process stack: Redis, Postgres, Prometheus, API, and
# N workers - the Docker-less equivalent of `docker compose up`. Every
# process is a normal background Windows process (no service registration,
# no admin rights needed); PIDs are recorded under run/ so stop_all.ps1 can
# tear everything down cleanly.
param(
    [int]$WorkerCount = 3
)
. "$PSScriptRoot\common.ps1"

Write-Host "=== Starting Redis ($RedisPort) ==="
if (-not (Get-SavedPid "redis")) {
    $p = Start-Process -FilePath (Join-Path $RedisDir "redis-server.exe") `
        -ArgumentList "--port", $RedisPort `
        -RedirectStandardOutput (Join-Path $LogDir "redis.log") `
        -RedirectStandardError (Join-Path $LogDir "redis.err.log") `
        -WindowStyle Hidden -PassThru
    Save-Pid "redis" $p.Id
    Start-Sleep -Seconds 2
}
& (Join-Path $RedisDir "redis-cli.exe") -p $RedisPort ping

Write-Host "=== Starting Postgres ($PgPort) ==="
$pgData = Join-Path $PgDir "pgdata"
if (-not (Test-Path $pgData)) {
    & (Join-Path $PgDir "pgsql\bin\initdb.exe") -D $pgData -U postgres --auth=trust --encoding=UTF8
}
& (Join-Path $PgDir "pgsql\bin\pg_ctl.exe") -D $pgData -l (Join-Path $LogDir "postgres.log") -o "-p $PgPort" start
Start-Sleep -Seconds 2
$dbExists = & (Join-Path $PgDir "pgsql\bin\psql.exe") -p $PgPort -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='jobsdb'"
if (-not $dbExists) {
    & (Join-Path $PgDir "pgsql\bin\createdb.exe") -p $PgPort -U postgres jobsdb
}

Write-Host "=== Creating tables ==="
& $VenvPython (Join-Path $Root "scripts\init_db.py")

Write-Host "=== Starting Prometheus ($PromPort) ==="
if (-not (Get-SavedPid "prometheus")) {
    $p = Start-Process -FilePath (Join-Path $PromDir "prometheus.exe") `
        -ArgumentList "--config.file=prometheus.yml", "--storage.tsdb.path=data" `
        -WorkingDirectory $PromDir `
        -RedirectStandardOutput (Join-Path $LogDir "prometheus.log") `
        -RedirectStandardError (Join-Path $LogDir "prometheus.err.log") `
        -WindowStyle Hidden -PassThru
    Save-Pid "prometheus" $p.Id
}

Write-Host "=== Starting API ($ApiPort) ==="
if (-not (Get-SavedPid "api")) {
    $p = Start-Process -FilePath $VenvPython `
        -ArgumentList "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", $ApiPort `
        -WorkingDirectory $Root `
        -RedirectStandardOutput (Join-Path $LogDir "api.log") `
        -RedirectStandardError (Join-Path $LogDir "api.err.log") `
        -WindowStyle Hidden -PassThru
    Save-Pid "api" $p.Id
}

Write-Host "=== Starting $WorkerCount workers ==="
$launcherPids = @{}   # name -> the pid Start-Process returned (a real, separate,
                       # still-alive process on this platform - NOT the pid the
                       # worker itself runs under; see comment below)
for ($i = 1; $i -le $WorkerCount; $i++) {
    $name = "worker-$i"
    if (-not (Get-SavedPid $name)) {
        $metricsPort = $WorkerMetricsBasePort + $i - 1
        $p = Start-Process -FilePath $VenvPython `
            -ArgumentList "-m", "worker.main", "--worker-id", $i, "--metrics-port", $metricsPort `
            -WorkingDirectory $Root `
            -RedirectStandardOutput (Join-Path $LogDir "$name.log") `
            -RedirectStandardError (Join-Path $LogDir "$name.err.log") `
            -WindowStyle Hidden -PassThru
        Save-Pid $name $p.Id
        $launcherPids[$name] = $p.Id
    }
}
# worker.main overwrites its own pid file with its real os.getpid() once it
# finishes interpreter startup and its imports (SQLAlchemy/redis/
# prometheus_client). On this platform, Start-Process's returned pid is a
# genuinely separate, still-alive process from the one the script actually
# runs under (observed directly: killing the Start-Process pid does not stop
# the worker) - so "is the saved pid alive" is not sufficient to detect the
# self-registration; wait for the saved pid to actually change away from the
# launcher pid. status.ps1/kill_worker.ps1 read this same file, so this is
# what makes their pids trustworthy immediately after start_all finishes.
$deadline = (Get-Date).AddSeconds(25)
while ((Get-Date) -lt $deadline -and $launcherPids.Count -gt 0) {
    $stillWaiting = @{}
    foreach ($name in $launcherPids.Keys) {
        $saved = Get-SavedPid $name
        if ($null -eq $saved -or $saved -eq $launcherPids[$name]) {
            $stillWaiting[$name] = $launcherPids[$name]
        }
    }
    $launcherPids = $stillWaiting
    if ($launcherPids.Count -eq 0) { break }
    Start-Sleep -Milliseconds 300
}
for ($i = 1; $i -le $WorkerCount; $i++) {
    $name = "worker-$i"
    $metricsPort = $WorkerMetricsBasePort + $i - 1
    Write-Host "  started $name pid=$(Get-SavedPid $name) metrics=:$metricsPort"
}

Start-Sleep -Seconds 2
Write-Host "`n=== Stack up ==="
Write-Host "API:        http://localhost:$ApiPort/docs"
Write-Host "Metrics:    http://localhost:$ApiPort/metrics"
Write-Host "Prometheus: http://localhost:$PromPort"
Write-Host "Run scripts\status.ps1 to check health, scripts\stop_all.ps1 to tear down."
