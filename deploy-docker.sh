#!/usr/bin/env sh
set -eu

echo "[EbbingFlow] Starting Docker one-click deployment..."

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not installed or not available in PATH." >&2
  exit 1
fi

docker compose version >/dev/null

if [ ! -f .env ] && [ -f .env.example ]; then
  cp .env.example .env
  echo "[EbbingFlow] Created .env from .env.example. Edit it later to set your model keys."
fi

docker compose up -d --build

cat <<'EOF'

[EbbingFlow] Deployment started.
Interaction Hub: http://localhost:8000
Data Monitor:     http://localhost:8000/monitor
Neo4j Browser:    http://localhost:7474

Useful commands:
  docker compose logs -f app
  docker compose down
EOF
