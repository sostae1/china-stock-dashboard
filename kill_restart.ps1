$ErrorActionPreference = 'SilentlyContinue'
$old = Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*backend*' }
foreach ($p in $old) {
    Write-Host "Killing PID $($p.ProcessId): $($p.CommandLine)"
    Stop-Process -Id $p.ProcessId -Force
}
Start-Sleep 2
Write-Host "Starting backend..."
Start-Process python -ArgumentList "C:\Users\Administrator\.qclaw\workspace\china-stock-data\backend.py" -WindowStyle Hidden
Write-Host "Done"
