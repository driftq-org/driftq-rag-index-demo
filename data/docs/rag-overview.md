# RAG Indexing Overview

RAG indexing usually looks like this:

1. Ingest documents
2. Chunk and clean text
3. Embed chunks into vectors
4. Upsert vectors into a vector database
5. At query time: embed the question, retrieve top-k chunks, then generate an answer

In this repo:
- `discover` loads input docs from `data/`
- `chunk` splits them into smaller pieces
- `embed` creates vectors (fake embeddings for demo speed)
- `upsert` writes to Qdrant
- `promote` switches the alias
- `smoketest` runs a few queries to verify it worked

Index versioning lets you build v2 while serving v1, then promote or roll back fast. It is the safest way to update search indexes in production.
