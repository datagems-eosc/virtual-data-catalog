import os
from pathlib import Path

S3_DIR = Path(os.environ.get("RESULTS_DIR", "/s3/data-model-management"))
S3_INPUTS_FOLDER = Path(os.environ.get("RESULTS_FOLDER", "ontop-inputs"))
S3_INPUTS_MAPPING_FOLDER = Path(
    os.environ.get("RESULTS_FOLDER", "ontop-inputs/mappings")
)
S3_INPUTS_ONTOLOGY_FOLDER = Path(
    os.environ.get("RESULTS_FOLDER", "ontop-inputs/ontologies")
)


def upload_ontop_properties(file_content: bytes, file_name: str):
    try:
        # Write the dataset file
        dataset_file = S3_DIR / S3_INPUTS_FOLDER / file_name

        # NOTE: If file name exists we overwrite the file silently
        with open(dataset_file, "wb") as f:
            # Save bytes to file
            f.write(file_content)

    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to results: {str(e)}")


def upload_ontology_file(file_content: bytes, file_name: str):
    try:
        # Write the dataset file
        dataset_file = S3_DIR / S3_INPUTS_ONTOLOGY_FOLDER / Path(file_name)

        # NOTE: If file name exists we overwrite the file silently
        with open(dataset_file, "wb") as f:
            # Save bytes to file
            f.write(file_content)

    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to results: {str(e)}")


def upload_mapping_file(file_content: bytes, file_name: str):
    try:
        # Write the dataset file
        dataset_file = S3_DIR / S3_INPUTS_MAPPING_FOLDER / file_name

        # NOTE: If file name exists we overwrite the file silently
        with open(dataset_file, "wb") as f:
            # Save bytes to file
            f.write(file_content)

    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to results: {str(e)}")
