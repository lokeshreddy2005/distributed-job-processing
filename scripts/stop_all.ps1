# Tears down everything start_all.ps1 started, in reverse dependency order.
. "$PSScriptRoot\common.ps1"

$WorkerCount = 10  # generous upper bound; Stop-SavedProcess no-ops if the pid file is absent
for ($i = 1; $i -le $WorkerCount; $i++) {
    Stop-SavedProcess "worker-$i"
}
Stop-SavedProcess "api"
Stop-SavedProcess "prometheus"

Write-Host "=== Stopping Postgres ==="
$pgData = Join-Path $PgDir "pgdata"
if (Test-Path $pgData) {
    try { & (Join-Path $PgDir "pgsql\bin\pg_ctl.exe") -D $pgData stop -m fast 2>$null } catch {}
}
Remove-Item (Join-Path $RunDir "postgres.pid") -ErrorAction SilentlyContinue

Write-Host "=== Stopping Redis ==="
try { & (Join-Path $RedisDir "redis-cli.exe") -p $RedisPort shutdown nosave 2>$null } catch {}
Remove-Item (Join-Path $RunDir "redis.pid") -ErrorAction SilentlyContinue

Write-Host "All stopped."
exit 0
