param(
  [string]$ServerHost = "127.0.0.1",
  [int]$Port = 8015,
  [string]$SampleCsv = "tests/sample_data.csv",
  [string]$DataDir = "data",
  [int]$StartupDelaySec = 6
)

# 1) Ensure venv & deps (run manually if needed):
# python -m venv .venv
# .\.venv\Scripts\Activate.ps1
# pip install -e .[dev]

# 2) Start server
$proc = Start-Process -FilePath "python" -ArgumentList "-m","uvicorn","app.main:app","--port",$Port -PassThru
Start-Sleep -Seconds $StartupDelaySec

# 3) Upload sample CSV (uses curl.exe if available, otherwise Invoke-RestMethod)
$uploadUrl = "http://${ServerHost}:${Port}/upload_csv"
Write-Host "Uploading $SampleCsv to $uploadUrl ..."
try {
  if (Get-Command curl.exe -ErrorAction SilentlyContinue) {
    $raw = & curl.exe -s -F ("file=@$SampleCsv") $uploadUrl
    $resp = $raw | ConvertFrom-Json
  } else {
    $form = @{ file = Get-Item $SampleCsv }
    $resp = Invoke-RestMethod -Uri $uploadUrl -Method Post -Form $form
  }
} catch {
  Write-Error "Upload failed: $_"; Stop-Process -Id $proc.Id -Force; exit 1
}

if (-not $resp.session_id) {
  Write-Error "No session_id returned: $($resp | ConvertTo-Json -Depth 5)"; Stop-Process -Id $proc.Id -Force; exit 1
}
$session = $resp.session_id
Write-Host "Session ID: $session"

# 4) Run pipeline endpoints
$base = "http://${ServerHost}:${Port}"
$endpoints = @("explore","hypotheses","execute","phase4/run")
foreach ($ep in $endpoints) {
  $url = "$base/$ep?session_id=$session"
  Write-Host "POST $url"
  try {
    $out = Invoke-RestMethod -Uri $url -Method Post
    $out | ConvertTo-Json -Depth 6 | Out-File -FilePath "$($ep -replace '/','_')_response.json" -Encoding utf8
  } catch {
    Write-Error "Request to $ep failed: $_"
  }
}

# 5) List data directory for session
$sessionPath = Join-Path $DataDir $session
Write-Host "Contents of $sessionPath"
if (Test-Path $sessionPath) {
  Get-ChildItem -Path $sessionPath | Select-Object Name,Length,LastWriteTime
} else {
  Write-Warning "$sessionPath not found."
}

# 6) Stop server
Write-Host "Stopping server (PID $($proc.Id))"
Stop-Process -Id $proc.Id -Force
