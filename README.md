# vdc-api

Virtual Data Catalog API (WP5)

The official documentation is available at: https://datagems-eosc.github.io/virtual-data-catalog/latest/

## 1. Getting started with your project

### Prerequisites

**Git LFS** is required to pull large files from the repository. Install it before cloning:

```bash
# macOS
brew install git-lfs

# Linux (Ubuntu/Debian)
sudo apt-get install git-lfs

# Then initialize Git LFS
git lfs install
git lfs pull
```

### Set Up Your Development Environment

#### Linux/macOS

If you do not have `uv` installed, you can install it with

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
After executing the command above, you will need to restart your shell.

`uv` is a python package similar to `poetry`.

Then, install the environment and the pre-commit hooks with

```bash
make install
```

This will also generate your `uv.lock` file

#### Windows

If you do not have `uv` installed, you can install it with

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
Or following the instructions [here](docs.astral.sh/uv/getting-started/installation/#installation-methods).

After executing the command above, you will need to restart your shell.

`uv` is a python package similar to `poetry`.

Then, install the environment and the pre-commit hooks with

```bash
uv sync
uv run pre-commit install
```

This will also generate your `uv.lock` file

## API Usage Examples

The dataset endpoint requires a bearer token and a dataset ID in the URL path.

Get API status:

```bash
curl -X GET "http://localhost:5002/api/v1"
```

Add a dataset:

```bash
curl -X POST "http://localhost:5002/api/v1/dataset/<dataset_id>" \
	-H "accept: application/json" \
	-H "Authorization: Bearer $TOKEN"
```

Open Swagger UI:

```bash
open "http://localhost:5002/api/v1/swagger"
```

## Running the API Locally
You can run the full stack with Docker Compose.

### Docker Compose

Start all services:

```bash
docker-compose up -d --build
```

Check service status:

```bash
docker-compose ps -a
```

Follow API logs:

```bash
docker-compose logs -f api
```

Stop services (keep data):

```bash
docker-compose down
```

Do not use `docker-compose down -v` unless you want to delete Docker volumes (including Dremio data).

## Services And Endpoints

When the Docker Compose stack is running, the following services are available:

| Service | Purpose | Host endpoint |
| --- | --- | --- |
| `api` | FastAPI service | `http://localhost:5002` |
| `dremio` | Dremio UI and catalog service | `http://localhost:9047` |
| `dremio` | Dremio Arrow Flight SQL endpoint | `localhost:32010` |
| `ontop` | Ontop SPARQL endpoint | `http://localhost:8080/sparql` (default, configurable via `SERVER_PORT`) |
| `ontop` | Ontop application root | `http://localhost:8080` (default, configurable via `SERVER_PORT`) |
| `dremio_init` | One-shot initialization job that bootstraps Dremio | No public endpoint |

Main API endpoints:

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `http://localhost:5002/api/v1` | API health/info |
| `POST` | `http://localhost:5002/api/v1/dataset/{dataset_id}` | Add dataset (requires Bearer token) |
| `GET` | `http://localhost:5002/api/v1/swagger` | Swagger UI |
