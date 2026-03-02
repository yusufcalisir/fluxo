Write-Host "Checking Docker status..." -ForegroundColor Cyan

$dockerInfo = docker info 2>&1

# Hata kodlarını ve "error during connect" uyarılarını kontrol edelim.
if ($LASTEXITCODE -ne 0 -or $dockerInfo -match "error during connect" -or $dockerInfo -match "Sistem belirtilen") {
    Write-Host "Docker is not running. Starting Docker Desktop..." -ForegroundColor Yellow
    
    $dockerPath = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    if (Test-Path $dockerPath) {
        Start-Process -FilePath $dockerPath
        Write-Host "Waiting for Docker engine to start... (This may take 30-60 seconds)" -ForegroundColor Yellow
        
        $maxAttempts = 60
        $attempt = 0
        $started = $false
        
        while ($attempt -lt $maxAttempts) {
            Start-Sleep -Seconds 2
            $null = docker info 2>&1
            if ($LASTEXITCODE -eq 0) {
                $started = $true
                break
            }
            Write-Host "." -NoNewline
            $attempt++
        }
        Write-Host ""
        
        if (-not $started) {
            Write-Host "Timed out waiting for Docker to start. Please check Docker Desktop manually." -ForegroundColor Red
            exit 1
        }
        Write-Host "Docker is ready!" -ForegroundColor Green
    } else {
        Write-Host "Docker Desktop not found at standard location ($dockerPath)." -ForegroundColor Red
        Write-Host "Please start Docker manually." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "Docker is already running." -ForegroundColor Green
}

Write-Host "Starting Fluxo via docker-compose..." -ForegroundColor Cyan
docker-compose up -d

$api_port = "8000"
$ui_port = "8501"

Write-Host "`nFluxo is starting up!" -ForegroundColor Green
Write-Host "UI Dashboard : http://localhost:$ui_port" -ForegroundColor Cyan
Write-Host "API Server   : http://localhost:$api_port" -ForegroundColor Cyan
