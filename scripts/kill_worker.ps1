# Failure-injection primitive: forcefully terminates one worker process
# (TerminateProcess via Stop-Process -Force - the Windows equivalent of
# kill -9, no chance for the process to ack in-flight work or clean up).
# Prints a UTC timestamp so it can be correlated against worker/API logs
# and the job_state_transitions table.
param(
    [Parameter(Mandatory = $true)][int]$Index
)
. "$PSScriptRoot\common.ps1"

$name = "worker-$Index"
$procId = Get-SavedPid $name
if ($null -eq $procId) {
    Write-Error "no pid recorded for $name - is it running?"
    exit 1
}
$ts = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
Write-Host "[$ts] KILLING $name (pid $procId) with SIGKILL-equivalent (Stop-Process -Force)"
Stop-Process -Id $procId -Force
Remove-Item (Join-Path $RunDir "$name.pid") -ErrorAction SilentlyContinue
Write-Host "[$ts] $name terminated."
