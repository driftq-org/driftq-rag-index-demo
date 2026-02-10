# Observability

If this were a real service, you would want to see:

- Structured logs (with run_id, step, and topic)
- Metrics (queue depth, processing time, success/failure counts)
- Tracing (so you can follow one request end-to-end)

In this demo you mostly have logs. That is fine for a demo, but if you ship this pattern, add metrics and traces early so failures are easier to debug.
