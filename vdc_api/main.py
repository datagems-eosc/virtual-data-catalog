import logging
import os
from fastapi import FastAPI
import uvicorn

from vdc_api.resources import ontop_configuration

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(
    title="Virtual Data Catalog API",
    description="API for the Virtual Data Catalog, allowing users to manage and query their data assets.",
    version="1.0.0",
    openapi_url="/api/v1/openapi.json",
    docs_url="/api/v1/swagger",
    redoc_url="/api/v1/redoc",
    root_path=os.getenv("ROOT_PATH", ""),
)

app.include_router(ontop_configuration.router, prefix="/api/v1")


@app.get("/api/v1")
def read_root():
    app_version = os.getenv("APP_VERSION", "dev")
    return {
        "message": f"API V1 is running (version: {app_version})",
        "endpoints": {
            "dataset": {
                "description": "Add a new dataset to dremio and ontop",
                "methods": ["POST"],
                "url": "/api/v1/dataset/{dataset_id}",
            },
            "mapping": {
                "description": "Add a new mapping for a specific dataset",
                "methods": ["POST"],
                "url": "/api/v1/mapping/{dataset_id}",
            },
        },
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("SERVER_PORT", 5000)))
