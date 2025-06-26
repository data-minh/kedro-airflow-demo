import inspect
import logging
from typing import Any, List, Optional, Union, Dict
from ..helper_functions import get_spark
from kedro_datasets.spark import SparkDataset
from pyspark.sql import DataFrame
from omegaconf import OmegaConf
from ..helper_functions import get_date_range


logger = logging.getLogger(__name__)

try:
    params = OmegaConf.load("./conf/base/globals.yml")
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

class FilteredSparkDataset(SparkDataset):
    """
    An extension of Kedro's `SparkDataset` that enables powerful and efficient
    data filtering directly at load time by leveraging Spark's partition pruning.

    This dataset can either dynamically construct SQL filter conditions based on a
    date range and a partition column, or it can apply pre-defined static filters.

    Example entry in `catalog.yml`:
    --------------------------------
    my_filtered_data:
      type: project.datasets.FilteredSparkDataset
      filepath: "s3a://my-bucket/data/partitioned_data"
      file_format: "parquet"
      load_args:
        # Dynamic filter: load the last 7 days of data based on PROCESSING_DATE
        range_date: 7
        partition_column: "processing_date"
        # Or, a static filter:
        # filters: "country = 'VN' AND age > 18"
    """
    def __init__(
        self,
        data_validation: Optional[bool] = None,
        **kwargs: Any
    ) -> None:
        """
        Initializes the FilteredSparkDataset.

        Args:
            **kwargs: Arguments from the catalog.yml, including special `load_args`
                      like `range_date`, `partition_column`, and `filters`.
        """
        super().__init__(**kwargs)

        self._range_date: Union[str, int] = self._load_args.get("range_date", "*")
        self._unit: str = self._load_args.get("unit", "day")
        self._data_validation = data_validation
        # --- Configuration Extraction, preserving original default logic ---
        self._partition_column: str = self._load_args.get("partition_column", "**")
        self._filters: Union[str, List[str]] = self._load_args.get("filters", "1=1")

        try:
            self._processing_date = str(params["PROCESSING_DATE"])
        except KeyError:
            function_name = inspect.currentframe().f_code.co_name
            raise ValueError(f"[{function_name}] Missing required parameter: 'PROCESSING_DATE'")

    def _load(self) -> DataFrame:
        """
        Loads data from the source, then applies dynamic or static filters.
        Spark's Catalyst Optimizer will attempt to push these filters down to the
        data source to optimize the read (i.e., partition pruning).

        Returns:
            A filtered Spark DataFrame.
        """
        spark = get_spark()
        read_obj = spark.read.format(self._file_format)

        # Get the load path (handled by the parent class)
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        # Apply schema if defined
        if self._schema:
             read_obj = read_obj.schema(self._schema)
        
        df = read_obj.load(load_path, **self._load_args)

        # Case: no filter is needed
        if (self._range_date == "*" or self._partition_column == "**") and self._filters == "1=1":
            return df

        filters_to_apply: List[str] = []

        # Case: filters are already provided
        if self._filters and self._filters != "1=1":
            if isinstance(self._filters, str):
                filters_to_apply.append(self._filters)
            elif isinstance(self._filters, list):
                filters_to_apply.extend(self._filters)
            else:
                raise ValueError(f"[DynamicFilterDataset] Invalid filters type: {type(self._filters)}")

        # Case: filters need to be generated from date range
        elif self._partition_column and self._range_date != "*":
            list_of_date = get_date_range(processing_date=self._processing_date, range_date=self._range_date, unit=self._unit)
            start_date, end_date = list_of_date[0], list_of_date[-1]
            if start_date == end_date:
                filters_to_apply.append(f"{self._partition_column} = DATE('{end_date}')")
            else:
                filters_to_apply.append(
                    f"{self._partition_column} >= DATE('{start_date}') AND "
                    f"{self._partition_column} <= DATE('{end_date}')"
                )

        # Apply all the collected filters
        for condition in filters_to_apply:
            df = df.filter(condition)

        return df

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._fs_prefix + str(self._filepath),
            "file_format": self._file_format,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
            "data_validation": self._data_validation,
            "type": "partitioned_parquet"
        }
