from mage_ai.io.file import FileIO
from pandas import DataFrame
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import os
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_file(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to filesystem.

    Docs: https://docs.mage.ai/design/data-loading#example-loading-data-from-a-file
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    project_id = 'data-engineering-afcon-2023'
    bucket_name = 'afcon_datalake'
    object_key = 'titanic_clean.csv.parquet'
    # filepath = 'titanic_clean.csv'
    # FileIO().export(df, filepath)
    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )