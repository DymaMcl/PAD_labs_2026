# Scenariul critic cu procese reale (Windows PowerShell).
# Utilizare:  .\scripts\scenariu_critic.ps1          (consumer idempotent)
#             .\scripts\scenariu_critic.ps1 -Naive   (fara deduplicare)
param([switch]$Naive)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$url = "http://localhost:7070"
$mode = @(); if ($Naive) { $mode = @("-naive") }

if (Test-Path data) { Remove-Item -Recurse -Force data }
New-Item -ItemType Directory -Force bin, data | Out-Null
go build -o bin/ ./cmd/...

Write-Host "== 1. pornesc brokerul (lease 3s) =="
$broker = Start-Process -FilePath .\bin\broker.exe -ArgumentList "-addr", ":7070", "-data", "data/broker.journal", "-lease", "3s" `
  -RedirectStandardError data\broker.log -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 1
try {
  Write-Host "== 2. producer: 5 alimentari x 100 pe ACC-1 (sold asteptat 500) =="
  .\bin\producer.exe -broker $url -count 5 -accounts 1 -interval 0 | Select-Object -Last 4

  Write-Host "== 3. consumer: cade dupa al 3-lea efect, fara ACK =="
  .\bin\consumer.exe -broker $url -name c1 @mode -crash-after-effect 3
  Write-Host "   -> consumer iesit cu codul $LASTEXITCODE"

  Write-Host "== 4. astept expirarea lease-ului si repornesc consumer-ul =="
  Start-Sleep -Milliseconds 3500
  $c = Start-Process -FilePath .\bin\consumer.exe -ArgumentList (@("-broker", $url, "-name", "c1") + $mode) -NoNewWindow -PassThru
  Start-Sleep -Seconds 3
  Stop-Process -Id $c.Id
  .\bin\consumer.exe -name c1 -show

  Write-Host "== 5. starea brokerului =="
  (Invoke-WebRequest "$url/v1/queues" -UseBasicParsing).Content
}
finally {
  Stop-Process -Id $broker.Id -ErrorAction SilentlyContinue
}
