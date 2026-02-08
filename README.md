# driftq-rag-index-demo

A **production-minded** demo repo that exercises **DriftQ-Core (as a Docker image)** as the task queue + redelivery engine, while building + versioning a small **Qdrant** RAG index.

## Architecture
![TODO](docs/flowchart.png)


## Quick start

```bash
cp .env.example .env
make up
make demo
```

### What `make demo` does

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


## Changing DriftQ-Core image

In `.env`:

```bash
DRIFTQ_IMAGE=ghcr.io/driftq-org/driftq-core:latest
```

Pin this to an exact tag/digest once you cut a v2.9+ release.


## Reset everything

```bash
make down
```
