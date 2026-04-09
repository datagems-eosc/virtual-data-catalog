from fastapi import APIRouter, HTTPException, status

router = APIRouter()


class MockResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


@router.post("/dataset", status_code=status.HTTP_201_CREATED)
async def add_dataset():
    """
    Add a new dataset to Dremio and Ontop.
    This endpoint allows you to add a new dataset to both Dremio and Ontop. The dataset will be created in Dremio, and the corresponding mappings will be set up in Ontop.
    - **dataset**: The dataset information, including name, description, source type, and source configuration.

    Returns a success message if the dataset was added successfully.
    """
    # Add dataset to Dremio
    dremio_response = await add_dataset_to_dremio()

    if dremio_response.status_code != 201:
        raise HTTPException(status_code=500, detail="Failed to add dataset to Dremio")

    # Add mappings to Ontop
    ontop_response = await add_mappings_to_ontop()

    if ontop_response.status_code != 201:
        raise HTTPException(status_code=500, detail="Failed to add mappings to Ontop")

    return {"message": "Dataset added successfully to Dremio and Ontop"}


async def add_dataset_to_dremio():
    print("Adding dataset to Dremio...")
    # Implement the logic to add the dataset to Dremio using its API
    # For example, you can use the requests library to send a POST request to Dremio's API endpoint for creating datasets
    # response = requests.post(dremio_api_url, json=dataset.dict())
    # return response
    return MockResponse(status_code=201)  # Mock response for demonstration


async def add_mappings_to_ontop():
    print("Adding mappings to Ontop...")
    # Implement the logic to add the corresponding mappings to Ontop using its API
    # For example, you can use the requests library to send a POST request to Ontop's API endpoint for creating mappings
    # response = requests.post(ontop_api_url, json=mapping_data)
    # return response
    return MockResponse(status_code=201)  # Mock response for demonstration
