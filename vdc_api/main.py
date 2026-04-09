import os
from fastapi import FastAPI
import uvicorn

app = FastAPI(
    title="Virtual Data Catalog API",
    description="API for the Virtual Data Catalog, allowing users to manage and query their data assets.",
    version="1.0.0",
    openapi_url="/api/v1/openapi.json",
    docs_url="/api/v1/swagger",
    redoc_url="/api/v1/redoc",
    root_path=os.getenv("ROOT_PATH", ""),
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
