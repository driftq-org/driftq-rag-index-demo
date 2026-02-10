# RAG Indexing Overview

Retrieval-Augmented Generation (RAG) often uses:
1) ingest documents
2) chunk and clean text
3) embed chunks into vectors
4) upsert into a vector database
5) query-time: embed query, retrieve top-k chunks, generate an answer

Index versioning lets you safely build v2 while serving v1, then promote/rollback instantly.
