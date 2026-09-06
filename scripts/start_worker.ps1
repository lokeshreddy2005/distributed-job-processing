# Starts (or restarts) a single worker by index. Used to restore pool
# capacity after kill_worker.ps1, or to scale the pool up.
param(
    [Parameter(Mandatory = $true)][int]$Index
)
. "$PSScriptRoot\common.ps1"

$name = "worker-$Index"
$metricsPort = $WorkerMetricsBasePort + $Index - 1
$p = Start-Process -FilePath $VenvPython `
    -ArgumentList "-m", "worker.main", "--worker-id", $Index, "--metrics-port", $metricsPort `
    -WorkingDirectory $Root `
    -RedirectStandardOutput (Join-Path $LogDir "$name.log") `
    -RedirectStandardError (Join-Path $LogDir "$name.err.log") `
    -WindowStyle Hidden -PassThru
Save-Pid $name $p.Id
# worker.main overwrites this same pid file with its own os.getpid() shortly
# after $VenvPython finishes interpreter startup (the venv's python.exe can
# be a launcher whose PID differs from the process that ends up running the
# script) - wait for that self-registration so the printed pid is the real,
# kill-able one, matching what status.ps1/kill_worker.ps1 will read.
$deadline = (Get-Date).AddSeconds(25)
$realPid = $p.Id
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 300
    $saved = Get-SavedPid $name
    if ($saved -and $saved -ne $p.Id -and (Get-Process -Id $saved -ErrorAction SilentlyContinue)) {
        $realPid = $saved
        break
    }
}
Write-Host "started $name pid=$realPid metrics=:$metricsPort"
