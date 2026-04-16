$ErrorActionPreference = 'SilentlyContinue'

# Kill old backend processes
Get-Process python | Stop-Process -Force

Start-Sleep 1

# Start Flask server
Write-Host "Starting Flask server..."
Start-Process python -ArgumentList "-m", "http.server", "5001" -WorkingDirectory "C:\Users\Administrator\.qclaw\workspace\china-stock-data" -WindowStyle Hidden

# Wait for server to start
Start-Sleep 2

# Start runner loop
Write-Host "Starting runner loop..."
Start-Process python -ArgumentList "C:\Users\Administrator\.qclaw\workspace\china-stock-data\runner_loop.py" -WindowStyle Hidden

Write-Host "All started"
