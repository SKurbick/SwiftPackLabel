FROM python:3.12-slim-bookworm

# Устанавливаем uv и системные зависимости
RUN pip install --no-cache-dir uv && \
    apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копируем зависимости
COPY pyproject.toml .

# Устанавливаем зависимости через uv
RUN uv pip install -r pyproject.toml --system --no-cache

# Копируем исходный код
COPY . .

# Команда запуска через uv (пример для FastAPI/Starlette)
CMD ["uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8001"]