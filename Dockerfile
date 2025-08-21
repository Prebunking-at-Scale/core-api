FROM python:3.13-slim-bookworm AS base

COPY --from=ghcr.io/astral-sh/uv:0.7.14 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# runtime dependencies
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends git; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY uv.lock pyproject.toml /app/
ADD core/ /app/core/
RUN uv sync --locked

EXPOSE 8000
CMD ["uv", "run", "litestar", "--app", "core.app:app", "run", "--host", "0.0.0.0", "--port", "8000", "-W", "10"]