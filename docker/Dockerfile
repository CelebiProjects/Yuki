# syntax=docker/dockerfile:1
# Multi-stage build for Yuki.
#   docker build --target dev  -t yuki:dev .
#   docker build --target prod -t yuki:<version> .   (prod is the default/final stage)

########## base ##########
# Snakemake >=8 requires Python >=3.11.
FROM python:3.11-slim AS base

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        rabbitmq-server \
        ca-certificates \
        krb5-user \
        xrootd-client \
        xrootd-plugins \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user; storage dir created here so named volumes
# inherit yuki ownership on first mount.
RUN useradd --create-home --uid 1000 --shell /bin/sh yuki && \
    mkdir -p /home/yuki/.Yuki && \
    chown -R yuki:yuki /home/yuki

RUN pip install --no-cache-dir build

COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 3315

########## dev ##########
# Editable install; docker-compose bind-mounts the repo over /app/Yuki
# (same path, so the editable install keeps working).
FROM base AS dev

WORKDIR /app/Yuki
COPY . /app/Yuki

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install celebichrono && \
    python docker/install-dependencies.py && \
    pip install --no-deps -e . && \
    chown -R yuki:yuki /app/Yuki

USER yuki
WORKDIR /app
CMD ["/app/entrypoint.sh"]

########## prod (final stage = default) ##########
FROM base AS prod

WORKDIR /app/Yuki-src
COPY . /app/Yuki-src

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install celebichrono && \
    python docker/install-dependencies.py && \
    python -m build && \
    pip install --no-deps dist/*.whl && \
    cd /app && rm -rf /app/Yuki-src

USER yuki
WORKDIR /app
CMD ["/app/entrypoint.sh"]
