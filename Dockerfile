# Install uv
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install git-lfs for large file support
RUN apt-get update && apt-get install -y git-lfs && rm -rf /var/lib/apt/lists/*

# Change the working directory to the `app` directory
WORKDIR /app

# Copy the lockfile and `pyproject.toml` into the image
COPY uv.lock /app/uv.lock
COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md

# Install dependencies
RUN uv sync --frozen --no-install-project

# Copy the project into the image
COPY vdc_api /app/vdc_api

# Sync the project
RUN uv sync --frozen

CMD ["uv", "run", "uvicorn", "vdc_api.main:app", "--host", "0.0.0.0", "--port", "5000"]
