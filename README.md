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

## Running the API Locally
You can run the API either directly using Docker.

## Services And Endpoints

When the Docker Compose stack is running, the following services are available:

| Service | Purpose | Host endpoint |
| --- | --- | --- |
| `postgres_db` | PostgreSQL database used by Dremio and Ontop | `localhost:5432` |
| `dremio` | Dremio UI and catalog service | `http://localhost:9047` |
| `dremio` | Dremio Arrow Flight SQL endpoint | `localhost:32010` |
| `ontop` | Ontop SPARQL endpoint | `http://localhost:9090/sparql` |
| `ontop` | Ontop application root | `http://localhost:9090` |
| `dremio_init` | One-shot initialization job that bootstraps Dremio and creates the PostgreSQL source | No public endpoint |

Default credentials and source names are configured in [`.env`](/Users/zoech/Documents/projects/datagems/code/virtual-data-catalog/.env):

| Setting | Value |
| --- | --- |
| `POSTGRES_DB` | `library` |
| `POSTGRES_USER` | `postgres_user` |
| `DREMIO_ADMIN_USER` | `vdc` |
| `DREMIO_POSTGRES_SOURCE_NAME` | `library` |

### Docker

Run the API using Docker.

Use the provided Dockerfile to build the image:

```bash
docker build -t fastapi-image .
```

Start a container from the image and mount the results directory:

```bash
docker run -d -p 5000:5000 -v /path/to/your/local/results:/app/dmm_api/data/results --name fastapi-container fastapi-image
```
