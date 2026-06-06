# Docker one-click deployment

This project can run with Docker Compose. Compose starts:

- `app`: EbbingFlow API and web UI on port `8000`
- `neo4j`: graph database on ports `7474` and `7687`

Runtime data is stored in Docker volumes, including `.data`, Chroma data, local SQLite files, Neo4j data, and local embedding model cache.

## Quick start

Windows PowerShell:

```powershell
.\deploy-docker.ps1
```

Linux/macOS:

```bash
chmod +x ./deploy-docker.sh
./deploy-docker.sh
```

Or run Compose directly:

```bash
docker compose up -d --build
```

Then open:

- Interaction Hub: http://localhost:8000
- Data Monitor: http://localhost:8000/monitor
- Neo4j Browser: http://localhost:7474

## Configuration

If `.env` does not exist, the deploy scripts copy `.env.example` to `.env`.

Important values:

```ini
OPENAI_API_KEY=sk-your-key-here
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-3.5-turbo

EMBED_TYPE=local
EMBED_MODEL=paraphrase-multilingual-MiniLM-L12-v2

NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=ebbingflow_password
```

Inside Compose, `NEO4J_URI` is set to `bolt://neo4j:7687` automatically.

If you use Ollama running on the host machine, set:

```ini
OPENAI_BASE_URL=http://host.docker.internal:11434/v1
OPENAI_MODEL=qwen2.5:7b
```

## Operations

View logs:

```bash
docker compose logs -f app
```

Stop services:

```bash
docker compose down
```

Stop and remove persisted volumes:

```bash
docker compose down -v
```
