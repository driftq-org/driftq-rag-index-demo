# driftq-rag-index-demo

A **production-minded** demo repo that exercises **DriftQ-Core (as a Docker image)** as the task queue + redelivery engine, while building + versioning a small **Qdrant** RAG index.

✅ **Zero local installs required** (no local Python/jq). The demo runs inside a Docker “runner” container.

## Architecture
![TODO](docs/flowchart.png)

## Prereqs
- Docker Desktop (or Docker Engine) + Docker Compose (the `docker compose` plugin)

Optional:
- `make` (nice wrapper, but not required)

## Quick start

```bash
cp .env.example .env
make demo
```

No `make`? Use Docker directly:

```bash
cp .env.example .env
docker compose up --build --abort-on-container-exit --exit-code-from demo demo
```

### What the demo does

1. Starts services (DriftQ-Core + Qdrant + API + Worker)
2. Kicks off a **build** that intentionally fails at the `embed` step
3. Replays the same run starting at `embed` (reuses cached step artifacts)
4. Builds a second version successfully and promotes it
5. Rolls back the "active" alias to the previous version

## Services

- **driftq**: DriftQ-Core container (HTTP API on `:8080`)
- **qdrant**: Vector DB (HTTP API on `:6333`)
- **api**: FastAPI demo API (HTTP API on `:8000`)
- **worker**: Background worker consuming DriftQ topics and running the pipeline
- **demo**: Runner container that executes `scripts/demo.sh` (so users don’t need local Python)

## API

- `POST /demo/build` → start a build (creates a run + enqueues a task in DriftQ)
- `POST /demo/replay` → replay an existing run from a step
- `POST /demo/rollback` → rollback the active alias to a prior version
- `GET  /demo/status/{run_id}` → run status + step artifacts
- `GET  /demo/index/{index}` → index history + currently active version (from Qdrant alias)

OpenAPI docs:
- http://localhost:8000/docs

## Notes on versioning & rollback

This demo uses a very standard, production-friendly approach:

- Each build writes into a **new Qdrant collection**: `demo_<index>_v<version>`
- Promoting a version updates the alias `demo_<index>_active` to the new collection
- Rollback just points the alias back to a previous collection

This is deliberate: the demo repo should be easy to understand, and it keeps "index versioning" aligned with real-world Qdrant deployments.

## Useful commands

Run the demo (recommended):

```bash
make demo
```

Bring services up and keep them running:

```bash
make up
```

Tail logs:

```bash
make logs
```

Reset everything (stop + wipe volumes):

```bash
make down
```

## Troubleshooting

### Demo can’t reach the API
The demo runner talks to the API over the Docker network (e.g. `http://api:8000`), not `localhost`. If you changed scripts, make sure they use `API_BASE_URL` and the `demo` service sets:

- `API_BASE_URL=http://api:8000`

### DriftQ topic-create errors
Pin `DRIFTQ_IMAGE` to a known compatible tag (instead of `:latest`) if you’re iterating on DriftQ’s API schema. Using `latest` is convenient but can break templates over time.
