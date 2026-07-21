import logging
import os
from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
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


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status_code": exc.status_code, "detail": exc.detail},
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"status_code": 500, "detail": str(exc)},
    )


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
            "ontop/ontology": {
                "description": "Add a new ontology to ontop",
                "methods": ["GET"],
                "url": "/api/v1/ontop/ontology",
            },
            "ontop/mapping": {
                "description": "Add a new mapping to ontop",
                "methods": ["GET"],
                "url": "/api/v1/ontop/mapping",
            },
            "ontop/properties": {
                "description": "Get ontop properties",
                "methods": ["GET"],
                "url": "/api/v1/ontop/properties",
            },
            "ontop/lenses": {
                "description": "Get ontop lenses",
                "methods": ["GET"],
                "url": "/api/v1/ontop/lenses",
            },
            "s3/upload": {
                "description": "Upload files to S3",
                "methods": ["POST"],
                "url": "/api/v1/s3/upload",
            },
            "query/sparql": {
                "description": "Execute SPARQL queries against the Ontop endpoint",
                "methods": ["POST"],
                "url": "/api/v1/query/sparql",
            },
        },
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("SERVER_PORT", 5000)))
