# Laboratorul 2 cu procese reale (Windows PowerShell): broker + serviciu de
# conturi gRPC care pierde 30% din raspunsuri DUPA executie + producer +
# consumer. La mijloc serviciul este oprit si repornit.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
if (Test-Path data) { Remove-Item -Recurse -Force data }
New-Item -ItemType Directory -Force bin, data | Out-Null
go build -o bin/ ./cmd/...

function Start-Accounts {
  Start-Process -FilePath .\bin\accounts.exe -PassThru -WindowStyle Hidden `
    -ArgumentList "-grpc", ":7080", "-http", ":7081", "-data", "data/accounts.journal", "-drop-rate", "0.3" `
    -RedirectStandardError data\accounts.log
}
$broker = Start-Process -FilePath .\bin\broker.exe -PassThru -WindowStyle Hidden `
  -ArgumentList "-addr", ":7070", "-data", "data/broker.journal", "-lease", "4s" -RedirectStandardError data\broker.log
$acc = Start-Accounts
Start-Sleep -Seconds 1
try {
  Write-Host "== producer: 12 alimentari pe 3 conturi =="
  .\bin\producer.exe -count 12 -accounts 3 -interval 0 | Select-Object -Last 6

  Write-Host "== consumer (efect prin serviciul gRPC) =="
  $cons = Start-Process -FilePath .\bin\consumer.exe -ArgumentList "-name", "c1" -PassThru -WindowStyle Hidden `
    -RedirectStandardError data\consumer.log
  Start-Sleep -Milliseconds 1500

  Write-Host "== opresc serviciul de conturi 3 secunde =="
  Stop-Process -Id $acc.Id
  Start-Sleep -Seconds 3
  .\bin\producer.exe -count 6 -accounts 3 -interval 0 -source producer-2 | Select-Object -Last 5

  Write-Host "== repornesc serviciul =="
  $acc = Start-Accounts
  Start-Sleep -Seconds 8
  Stop-Process -Id $cons.Id

  Write-Host "== solduri in serviciu (asteptat 600 pe fiecare cont) =="
  (Invoke-WebRequest "http://localhost:7081/admin/state" -UseBasicParsing).Content | ConvertFrom-Json | Select-Object -ExpandProperty balances
  Write-Host "Jurnalul consumer-ului: data\consumer.log"
}
finally {
  Stop-Process -Id $broker.Id, $acc.Id -ErrorAction SilentlyContinue
}
