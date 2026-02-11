# driftq-rag-index-demo

This repo is a small, production-minded demo that shows how to:
- use DriftQ-Core (via Docker) as a task queue + redelivery engine
- build and version a tiny Qdrant RAG index
- replay failed runs safely
- roll back an index by switching a Qdrant alias

It is designed to be easy to run locally. You do not need local Python or jq. The demo runs inside a Docker runner container.

## What you get from this demo

- A minimal FastAPI service that kicks off builds and exposes run status
- A background worker that consumes DriftQ topics and runs the pipeline
- A Qdrant vector DB with versioned collections and an alias
- A scripted demo that exercises the full lifecycle

## Architecture

![DriftQ RAG demo flow](docs/flowchart.png)

- Client/Demo calls the FastAPI service over HTTP to start builds, replay runs, or request rollback.
- The API creates a run record in `/state`, then **produces** a message to DriftQ (it does not consume).
- DriftQ holds the queue and handles redelivery. The worker **consumes** tasks, and then **acks or nacks** them.
- The worker runs the pipeline steps (`discover -> chunk -> embed -> upsert -> promote -> smoketest`).
- Each build writes vectors into a new Qdrant collection: `demo_<index>_v<version>`.
- On success, the worker updates the Qdrant alias `demo_<index>_active` to point to the new collection.
- Rollback is just another DriftQ task: the worker flips the alias back to a prior version.

## Prereqs

- Docker Desktop (or Docker Engine) with the `docker compose` plugin
- `make` is optional (just a nice wrapper)

## Quick start

```bash
make demo
```

If `.env` is missing, `make demo` creates it from `.env.example` automatically.
To customize values first, run `cp .env.example .env` and edit `.env`.

No `make`? Use Docker directly:

```bash
docker compose up --build --abort-on-container-exit --exit-code-from demo demo
```

## What the demo does

1. Starts services (DriftQ-Core + Qdrant + API + Worker)
2. Kicks off a build that intentionally fails at the `embed` step
3. Replays the same run starting at `embed` (reuses cached artifacts)
4. Builds a second version successfully and promotes it
5. Rolls back the active alias to the previous version

## Services

- `driftq`: DriftQ-Core container (HTTP API on `:8080`)
- `qdrant`: Vector DB (HTTP API on `:6333`)
- `api`: FastAPI demo API (HTTP API on `:8000`)
- `worker`: Background worker consuming DriftQ topics and running the pipeline
- `demo`: Runner container that executes `scripts/demo.sh`

## API endpoints

- `POST /demo/build`
  - Starts a new build
  - Creates a run state file under `/state`
  - Enqueues a task on DriftQ

- `POST /demo/replay`
  - Replays a prior run from a specific step
  - Reuses cached artifacts from earlier steps

- `POST /demo/rollback`
  - Moves the active alias back to a previous version
  - This is done via DriftQ (so it is queued and retriable)

- `GET /demo/status/{run_id}`
  - Returns the full run state (steps, artifacts, status)

- `GET /demo/index/{index}`
  - Shows index history and current alias target

OpenAPI docs:
- http://localhost:8000/docs

## Pipeline steps (in order)

1. `discover`: load the input dataset
2. `chunk`: split docs into small chunks
3. `embed`: turn chunks into vectors
4. `upsert`: write vectors to Qdrant
5. `promote`: point the alias to the new collection
6. `smoketest`: run a few queries to verify indexing

## Versioning and rollback

Each build writes into a new Qdrant collection:
- `demo_<index>_v<version>`

The active collection is referenced by an alias:
- `demo_<index>_active`

Promote moves the alias to the new collection. Rollback moves it back to a previous one. This is simple and reliable in production.

## State storage

Run state is stored on a shared volume under `/state`:
- `/state/runs/<run_id>/state.json` (status, steps, artifacts)
- `/state/runs/<run_id>/log.txt` (human-readable log)
- `/state/indexes/<index>/history.json` (versions and active pointer)

## How the worker loop handles retries

- The worker consumes messages from DriftQ using a consumer group.
- If the handler succeeds, it `ack`s the message.
- If the handler fails, it `nack`s the message so DriftQ can redeliver it.
- A failed run stays failed unless you explicitly replay it.
- A replay clears old errors and re-runs from a chosen step.

This is the same retry pattern you would use in production: retry the unit of work, keep state stable, and make replays explicit.

## FAQ

**Why does the first build intentionally fail?**
This demo shows how a failed build can be replayed safely. The scripted demo always injects one failure at the `embed` step, then replays from `embed` and succeeds.

**Why does the demo run inside a container?**
So you do not need Python or other local tools. The runner container calls the API and checks run status for you.

**What if the demo waits forever for FAILED?**
That usually means you already used the �fail once� and the run succeeded. Run `make down` to wipe volumes and try again.

**What is the data source?**
Sample docs live under `data/docs/`. The pipeline reads these files and uses them to build embeddings.

**Is this production code?**
No. The design is production-inspired, but the implementation is intentionally small and easy to read.

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

### Troubleshooting by symptom

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Demo waits for FAILED but keeps showing SUCCEEDED | The �fail once� was already consumed or old state is reused | Run `make down`, then `make demo` |
| `enqueue_failed` / 502 from `/demo/build` | DriftQ schema mismatch (produce payload rejected) | Pin `DRIFTQ_IMAGE` to a known compatible tag and rebuild |
| Worker logs show `missing lease` | DriftQ consume response shape changed | Pin DriftQ image or adjust lease extraction in `api/app/driftq_client.py` |
| `alias_target` is `null` right after rollback | Alias update race | Call `/demo/index/{index}` again after a few seconds |
| Demo cannot reach API | Runner uses Docker network, not localhost | Ensure `API_BASE_URL=http://api:8000` in `demo` service |

### Demo cannot reach the API
The demo runner talks to the API over the Docker network (for example, `http://api:8000`), not `localhost`. If you changed scripts, make sure they use `API_BASE_URL` and the `demo` service sets:

- `API_BASE_URL=http://api:8000`

### DriftQ topic-create errors
DriftQ has had small schema differences across versions. If you see topic-create or produce errors, pin `DRIFTQ_IMAGE` to a known compatible tag in `.env` (instead of `:latest`).

### Expected FAILED but got SUCCEEDED during demo
This usually means the failure was already consumed in a previous run or state was reused. Run `make down` to wipe volumes and try again.

## How to extend this demo

Here are a few simple ways to make this more real without making it complicated:

- Replace `fake_embed()` with a real embedding model
- Add auth and rate limits to the API
- Add metrics (Prometheus) and traces (OpenTelemetry)
- Add a second worker to see parallel consumption
- Make the dataset larger and watch how batching affects Qdrant upserts
