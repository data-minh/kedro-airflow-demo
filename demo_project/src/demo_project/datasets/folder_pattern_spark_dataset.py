from typing import Any, Dict, List, Optional, Union
from kedro_datasets.spark import SparkDataset
from pyspark.sql import DataFrame
from ..helper_functions import get_spark
from omegaconf import OmegaConf
import inspect
from datetime import datetime, timedelta
import logging
from ..helper_functions import get_date_range_for_folder_pattern


logger = logging.getLogger(__name__)

try:
    params = OmegaConf.load("./conf/base/parameters.yml")
except Exception as e:
    logger.error(f"Failed to load globals.yml: {e}")
    params = {}

def _strip_dbfs_prefix(path: str, prefix: str = "/dbfs") -> str:
    """
    Removes the '/dbfs' prefix from a file path.
    Useful for Databricks environments where the Spark API does not need the prefix.

    Args:
        path: The input file path.
        prefix: The prefix string to remove.

    Returns:
        The path with the prefix removed.
    """
    return path[len(prefix):] if path.startswith(prefix) else path

class FolderPatternSparkDataset(SparkDataset):

    def __init__(
        self,
        data_validation: Optional[bool] = None,
        **kwargs: Any
    ) -> None:

        super().__init__(**kwargs)

        self._range_date: Union[str, int] = self._load_args.get("range_date", "*")
        self._unit: str = self._load_args.get("unit", "day")
        self._data_validation = data_validation

        # Get the load path (handled by the parent class)
        self.load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        try:
            self._processing_date = str(params["PROCESSING_DATE"])
            self._processing_date = datetime.strptime(str(params["PROCESSING_DATE"]), '%Y-%m-%d').date().strftime('%Y-%m-%d')
        except KeyError:
            function_name = inspect.currentframe().f_code.co_name
            raise ValueError(f"[{function_name}] Missing required parameter: 'PROCESSING_DATE'")


    def _load(self) -> DataFrame:

        spark = get_spark()
        read_obj = spark.read.format(self._file_format)

        # Apply schema if defined
        if self._schema:
             read_obj = read_obj.schema(self._schema)

        full_paths: Any = None

        if isinstance(self._range_date, int) and self._range_date != '*':
            folders = get_date_range_for_folder_pattern(processing_date=self._processing_date, range_date=self._range_date, unit=self._unit)
            folder_glob_pattern = "{" + ",".join(folders) + "}"
            full_paths = f"{self.load_path}/{folder_glob_pattern}"
        else:
            full_paths = f"{self.load_path}/*" 
        df = read_obj.option("basePath", self.load_path).load(full_paths, **self._load_args)
        
        return df
    
    def _save(self, data: DataFrame) -> None:
        save_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_save_path()))
        full_save_path = save_path + '/' + self._processing_date

        data.write.save(full_save_path, self._file_format, **self._save_args)

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._fs_prefix + str(self._filepath),
            "file_format": self._file_format,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
            "data_validation": self._data_validation,
            "type": "folder_pattern"
        }