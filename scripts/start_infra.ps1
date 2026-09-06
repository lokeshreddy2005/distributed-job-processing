# Starts only Redis + Postgres (no API/workers/Prometheus) - enough to run
# the test suite (tests/conftest.py) against real backends.
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

foreach ($db in @("jobsdb", "jobsdb_test")) {
    $exists = & (Join-Path $PgDir "pgsql\bin\psql.exe") -p $PgPort -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'"
    if (-not $exists) {
        & (Join-Path $PgDir "pgsql\bin\createdb.exe") -p $PgPort -U postgres $db
    }
}
Write-Host "Infra up: redis://localhost:$RedisPort  postgres://localhost:$PgPort/{jobsdb,jobsdb_test}"
