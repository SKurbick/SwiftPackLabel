FROM python:3.12-slim-bookworm

RUN pip install --no-cache-dir uv && \
    apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .

RUN uv pip install -r pyproject.toml --system --no-cache

COPY . .

CMD ["uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8001"]