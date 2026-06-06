$ErrorActionPreference = "Stop"

Write-Host "[EbbingFlow] Starting Docker one-click deployment..."

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker is not installed or not available in PATH."
}

docker compose version | Out-Null

if (-not (Test-Path ".env") -and (Test-Path ".env.example")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[EbbingFlow] Created .env from .env.example. Edit it later to set your model keys."
}

docker compose up -d --build

Write-Host ""
Write-Host "[EbbingFlow] Deployment started."
Write-Host "Interaction Hub: http://localhost:8000"
Write-Host "Data Monitor:     http://localhost:8000/monitor"
Write-Host "Neo4j Browser:    http://localhost:7474"
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  docker compose logs -f app"
Write-Host "  docker compose down"
