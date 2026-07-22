import logging
from typing import Iterator

import pyarrow as pa
from pydantic import BaseModel

from pipeline.models.pyarrow import schema_from_pydantic
from pipeline.storage.object_storage import ObjectStorage


logger = logging.getLogger(f'pipeline.{__name__}')


class NdjsonReader:
    """
    A utility class to read and combine multiple NDJSON files from a folder into a single pandas DataFrame.
    """

    def __init__(self, storage: ObjectStorage):
        """
        Initializes the reader with a target folder.

        Args:
            storage (ObjectStorage): The object storage to read from.
        """
        self.storage = storage

    def read(
        self,
        folder_path: str,
        api_model: BaseModel,
        extension: str = 'ndjson.gz',
    ) -> Iterator[pa.Table]:
        """
        Reads all found NDJSON files and concatenates them into one DataFrame.

        Args:
            folder_path (Path): Path to the directory containing the files.
            api_model (Pydantic.BaseModel): The Pydantic API model used to write the NDJSON files.
            extensions (str): File extensions to search for among '.ndjson', '.jsonl', '.ndjson.gz' and '.jsonl.gz'.

        Returns:
            An iterator on PyArrow tables.
        """
        files = self.storage.list(folder_path, extension)

        if not files:
            logger.warning('Warning: No matching NDJSON files found in %s', folder_path)

        for file_path in files:
            yield pa.json.read_json(
                file_path,
                parse_options=pa.json.ParseOptions(
                    explicit_schema=schema_from_pydantic(api_model)
                ),
            )
