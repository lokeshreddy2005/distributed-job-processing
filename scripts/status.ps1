# Reports whether each recorded process is still alive.
. "$PSScriptRoot\common.ps1"

Get-ChildItem $RunDir -Filter "*.pid" | ForEach-Object {
    $name = $_.BaseName
    $procId = [int](Get-Content $_.FullName)
    $p = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if ($p) {
        Write-Host "$name`: RUNNING (pid $procId)" -ForegroundColor Green
    } else {
        Write-Host "$name`: DEAD (stale pid $procId)" -ForegroundColor Red
    }
}
