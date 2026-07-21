import logging

import pandas as pd

from pipeline.datalake import DataLake, Dataset
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

    def read_all(
        self,
        folder_path: str,
        extension: str = 'ndjson.gz',
        columns: list = [],
        **kwargs,
    ) -> pd.DataFrame:
        """
        Reads all found NDJSON files and concatenates them into one DataFrame.

        Args:
            folder_path (Path): Path to the directory containing the files.
            extensions (str): File extensions to search for among '.ndjson', '.jsonl', '.ndjson.gz' and '.jsonl.gz'
            columns (list[str]): Optional list of column names to keep (saves memory).
            kwargs (Any): Additional arguments passed directly to pd.read_json() (e.g., encoding='utf-8', dtype={'id': int}).

        Returns:
            A single combined pandas DataFrame.
        """
        print(folder_path)
        files = self.storage.list(folder_path, extension)

        if not files:
            logger.warning('Warning: No matching NDJSON files found in %s', folder_path)

        df_list = []
        for file in files:
            try:
                df_chunk = pd.read_json(file, lines=True, compression='infer', **kwargs)

                if len(columns):
                    existing_cols = [c for c in columns if c in df_chunk.columns]
                    df_chunk = df_chunk[existing_cols]

                if not df_chunk.empty:
                    df_list.append(df_chunk)

            except Exception as e:
                logging.error('Error reading file %s: %s', file, e)
                continue

        if not df_list:
            return pd.DataFrame()

        return pd.concat(df_list, ignore_index=True)
