import logging
import os

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
import vdc_api.resources.security as security
import vdc_api.tools.mapping.mapping_generation as mapping_generation
import docker


router = APIRouter()
logger = logging.getLogger(__name__)


class MockResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


DREMIO_BASE_URL = (
    f"http://{os.getenv('DREMIO_HOST', 'dremio')}:{os.getenv('DREMIO_PORT', '9047')}"
)
DREMIO_ADMIN_USER = os.getenv("DREMIO_ADMIN_USER", "vdc")
DREMIO_ADMIN_PASSWORD = os.getenv("DREMIO_ADMIN_PASSWORD", "vdc-admin1")

DMM_URL = os.getenv("DMM_URL")
DMM_API_TIMEOUT_SECONDS = float(os.getenv("DMM_API_TIMEOUT_SECONDS", "300"))


@router.post("/dataset/{dataset_id}", status_code=status.HTTP_201_CREATED)
async def add_dataset(dataset_id: str, token: str = Depends(security.oauth2_scheme)):
    """
    Add a new dataset to Dremio.
    The dataset will be created in Dremio based on the information retrieved from the DMM API for the given dataset_id.
    - **dataset_id**: The ID of the dataset to add.
    Returns a success message if the dataset was added successfully.
    """
    dataset_info = await get_dataset_info(dataset_id, token)
    source_name = dataset_id

    # Add dataset to Dremio
    dremio_response = await add_dataset_to_dremio(
        dataset_id, token, dataset_info, source_name
    )
    if dremio_response.status_code != 201:
        logger.error(
            "Dremio dataset creation failed for dataset_id=%s with status_code=%s",
            dataset_id,
            dremio_response.status_code,
        )
        raise HTTPException(status_code=500, detail="Failed to add dataset to Dremio")

    # Add mappings to Onto only if the status of the dataset is "ready"
    status = "unknown"
    for node in dataset_info.get("nodes", []):
        if node.get("properties", {}).get("type") == "sc:Dataset":
            status = node.get("properties", {}).get("status")

    if status == "ready":
        await add_mappings_to_ontop(dataset_info, source_name)

    return {
        "message": "Dataset added successfully to Dremio and mappings updated in Ontop if dataset is ready"
    }


async def add_dataset_to_dremio(
    dataset_id: str, token: str, dataset_info: dict = None, source_name: str = ""
) -> MockResponse:
    """
    Add a new dataset to Dremio based on the mimeType.
    Supported mimeTypes: "text/sql" for PostgreSQL databases (and "text/csv" for CSV files in S3 in progress).
    """
    dremio_token = await get_dremio_token()

    mimeType = get_mimeType_for_dataset(dataset_info)
    logger.info("Dataset_id=%s has mimeType=%s", dataset_id, mimeType)
    if mimeType == "text/csv":
        created = await create_csv_source(dremio_token, dataset_id)
    elif mimeType == "text/sql":
        db_name = get_db_name_for_dataset(dataset_info)
        created = await create_postgres_source(dremio_token, db_name, source_name)
    else:
        logger.error("Unsupported mimeType=%s for dataset_id=%s", mimeType, dataset_id)
        return MockResponse(status_code=400)

    if created:
        return MockResponse(status_code=201)

    return MockResponse(status_code=500)


async def add_mappings_to_ontop(dataset_info: dict, source_name: str):
    """
    Generate mapping file for a given dataset and merge it with existing mappings in Ontop, then restart the Ontop container to apply the changes.
    #TODO: We should find where the mappings should be stored and avoid merging all files on each request. Check about multiple input mappings files with ontop
    """
    mapping_generation.generate_mappings(dataset_info, source_name=source_name)
    mapping_generation.merge_mapping_files()
    mapping_generation.merge_ontology_files()

    try:
        client = docker.from_env()
        container = client.containers.get("ontop-endpoint")
        container.restart()
        logger.info("Ontop container restarted successfully")
    except Exception:
        logger.exception("Failed to restart Ontop container")

    return MockResponse(status_code=201)  # Mock response for demonstration


def get_db_name_for_dataset(dataset_info: dict) -> str:
    """Retrieve the database name from the dataset_info by looking for a node of type "dg:DatabaseConnection" and extracting its name property."""
    name = ""
    for node in dataset_info.get("nodes", []):
        if node.get("properties", {}).get("type") != "dg:DatabaseConnection":
            continue

        name = node.get("properties", {}).get("name")
        if isinstance(name, str) and name:
            return name
        if isinstance(name, dict):
            value = name.get("@value") or name.get("value")
            if isinstance(value, str) and value:
                return value

    return name


async def create_postgres_source(token: str, db_name: str, source_name: str) -> bool:
    """Create a PostgreSQL source in Dremio with the given database name and source name.
    The connection details are retrieved from environment variables.
    """
    pg_payload = {
        "entityType": "source",
        "name": source_name,
        "type": "POSTGRES",
        "config": {
            "hostname": os.getenv("POSTGRES_HOST"),
            "port": int(os.getenv("POSTGRES_PORT")),
            "databaseName": db_name,
            "username": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD", ""),
            "useSsl": False,
        },
    }

    url = f"{DREMIO_BASE_URL}/api/v3/catalog"
    headers = {
        "Authorization": f"_dremio{token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30)) as client:
            response = await client.post(url, headers=headers, json=pg_payload)
    except httpx.RequestError:
        logger.exception(
            "Dremio catalog request failed for source_name=%s", source_name
        )
        return False
    if response.status_code == 409:
        logger.warning(
            "PostgreSQL source already exists in Dremio for source_name=%s", source_name
        )
        return True
    if response.status_code in (200, 201):
        return True

    logger.error(
        "Dremio source creation failed for source_name=%s, status=%s, body=%s",
        source_name,
        response.status_code,
        response.text[:500],
    )
    return False


async def create_csv_source(token: str, dataset_id: str) -> bool:
    """Create a CSV source in Dremio with the given dataset ID. (in progress, not tested yet)
    The connection details are retrieved from environment variables.
    """
    source_name = os.getenv("S3_SOURCE_NAME", f"s3-csv-{dataset_id}")
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        logger.error("Missing required S3_BUCKET for CSV source creation")
        return False

    endpoint = os.getenv("S3_ENDPOINT")
    region = os.getenv("S3_REGION", "us-east-1")
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")

    credential_type = "ACCESS_KEY" if access_key and secret_key else "NONE"

    property_list = [
        {"name": "fs.s3a.endpoint.region", "value": region},
        {
            "name": "fs.s3a.path.style.access",
            "value": os.getenv("S3_PATH_STYLE", "false"),
        },
        {
            "name": "fs.s3a.connection.ssl.enabled",
            "value": os.getenv("S3_SSL_ENABLED", "true"),
        },
        {"name": "store.s3.async", "value": "true"},
    ]
    if endpoint:
        property_list.append({"name": "fs.s3a.endpoint", "value": endpoint})

    s3_payload = {
        "entityType": "source",
        "name": source_name,
        "type": "S3",
        "config": {
            "credentialType": credential_type,
            "accessKey": access_key or "",
            "accessSecret": secret_key or "",
            "secure": os.getenv("S3_SSL_ENABLED", "true").lower() == "true",
            "compatibilityMode": bool(endpoint),
            "whitelistedBuckets": [bucket],
            "propertyList": property_list,
        },
    }

    logger.info(
        "Creating S3 CSV source in Dremio: source_name=%s bucket=%s endpoint=%s region=%s credential_type=%s",
        source_name,
        bucket,
        endpoint or "aws-default",
        region,
        credential_type,
    )

    url = f"{DREMIO_BASE_URL}/api/v3/catalog"
    headers = {
        "Authorization": f"_dremio{token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30)) as client:
            response = await client.post(url, headers=headers, json=s3_payload)
    except httpx.RequestError:
        logger.exception(
            "Dremio S3 source request failed for dataset_id=%s", dataset_id
        )
        return False

    if response.status_code in (200, 201, 409):
        return True

    logger.error(
        "Dremio S3 source creation failed for dataset_id=%s, status=%s, body=%s",
        dataset_id,
        response.status_code,
        response.text[:500],
    )
    return False


async def get_dremio_token() -> str:
    if not DREMIO_ADMIN_USER or not DREMIO_ADMIN_PASSWORD:
        raise HTTPException(
            status_code=500,
            detail="Missing Dremio admin credentials in environment",
        )

    url = f"{DREMIO_BASE_URL}/apiv2/login"
    payload = {"userName": DREMIO_ADMIN_USER, "password": DREMIO_ADMIN_PASSWORD}

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15)) as client:
            response = await client.post(url, json=payload)
    except httpx.RequestError as exc:
        logger.exception("Failed to contact Dremio login endpoint")
        raise HTTPException(
            status_code=502, detail="Cannot reach Dremio login endpoint"
        ) from exc

    if response.status_code != 200:
        logger.error(
            "Dremio login failed with status=%s body=%s",
            response.status_code,
            response.text[:500],
        )
        raise HTTPException(status_code=502, detail="Dremio authentication failed")

    token = response.json().get("token")
    if not token:
        raise HTTPException(
            status_code=502, detail="Missing token in Dremio login response"
        )

    return token


async def get_dataset_info(dataset_id: str, token) -> dict:
    """
    Get the dataset information from the DMM API for the given dataset_id. The token is used for authentication with the DMM API.
    Returns the dataset information as a dictionary if the request is successful, or raises an HTTPException.
    """
    request_url = f"{DMM_URL}/dataset/get/{dataset_id}"

    try:
        timeout = httpx.Timeout(DMM_API_TIMEOUT_SECONDS)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(
                request_url,
                headers={
                    "accept": "application/json",
                    "Authorization": f"Bearer {token}",
                },
            )

            if response.status_code == 200:
                dataset_info = response.json().get("dataset", {})

            else:
                logger.warning(
                    "DMM request failed for dataset_id=%s with status_code=%s and body=%s",
                    dataset_id,
                    response.status_code,
                    response.text[:500],
                )
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text or "Dataset not found in DMM",
                )
    except httpx.ReadTimeout as exc:
        logger.exception(
            "Timed out waiting for DMM response for dataset_id=%s after %ss",
            dataset_id,
            DMM_API_TIMEOUT_SECONDS,
        )
        raise HTTPException(
            status_code=504,
            detail=f"Timed out waiting for DMM API after {DMM_API_TIMEOUT_SECONDS} seconds",
        ) from exc
    except httpx.RequestError as e:
        logger.exception("RequestError while calling DMM for dataset_id=%s", dataset_id)
        raise HTTPException(
            status_code=500, detail=f"Error connecting to DMM API: {str(e)}"
        )

    if not isinstance(dataset_info, dict):
        logger.error(
            "Unexpected DMM payload type for dataset_id=%s: %s",
            dataset_id,
            type(dataset_info).__name__,
        )
        raise HTTPException(
            status_code=502,
            detail="Unexpected response format from DMM API",
        )

    return dataset_info


def get_mimeType_for_dataset(dataset_info: dict) -> str:
    """Retrieve the mimeType from the dataset_info by looking for a node of type "cr:FileObject" and extracting its encodingFormat property."""
    for node in dataset_info.get("nodes", []):
        if node.get("properties", {}).get("type") == "cr:FileObject":
            mimeType = node.get("properties", {}).get("encodingFormat")
            if isinstance(mimeType, str) and mimeType:
                return mimeType
            if isinstance(mimeType, dict):
                value = mimeType.get("@value") or mimeType.get("value")
                if isinstance(value, str) and value:
                    return value
    return ""
