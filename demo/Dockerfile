FROM python:3.12-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends bash curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /work
ENTRYPOINT ["bash", "scripts/demo.sh"]
