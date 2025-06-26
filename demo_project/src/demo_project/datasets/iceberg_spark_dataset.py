from typing import Any, List, Optional, Union, Dict
from kedro_datasets.spark import SparkDataset
from ..helper_functions import get_spark
from pyspark.sql import DataFrame
import logging
import inspect
from omegaconf import OmegaConf
from ..helper_functions import get_date_range


logger = logging.getLogger(__name__)

try:
    params = OmegaConf.load("./conf/base/globals.yml")
except Exception as e:
    logger.error(f"Failed to load globals.yml: {e}")
    params = {}

try:
    from airflow.models import Variable
    from ..helper_functions import is_valid_date_format
    processing_date = Variable.get("PROCESSING_DATE")
    if processing_date and is_valid_date_format(processing_date):
        params["PROCESSING_DATE"] = processing_date
except:
    pass

class IcebergSparkDataset(SparkDataset):
    """
    A Kedro Dataset to interact with Apache Iceberg tables via Spark.

    This dataset handles both reading from and writing to Iceberg tables.
    It supports dynamic date-based filtering on load, leveraging Iceberg's
    partition pruning capabilities. It uses the modern Iceberg v2 Spark API
    (`df.writeTo`) for save operations.

    Example entry in `catalog.yml`:
    --------------------------------
    my_iceberg_table:
      type: project.datasets.IcebergSparkDataset
      load_args:
        catalog: "my_catalog"
        namespace: "my_db"
        table_name: "my_table"
        # Optional: Dynamic filter for the last 7 days
        range_date: 7
        partition_column: "event_date"
      save_args:
        mode: "overwrite" # or "append"
        partitionBy: ["event_date", "country"]
    """

    def __init__(
        self,
        *,
        data_validation: Optional[bool] = None,
        **kwargs: Any
    ):
        """
        Initializes the IcebergSparkDataset.
        """
        
        # Ensure 'file_format' is set to 'iceberg'
        # Ensure file format is 'iceberg'
        if kwargs.get("file_format") != "iceberg":
            logger.warning("Overriding 'file_format' to 'iceberg'.")
        kwargs["file_format"] = "iceberg"

        # Since Iceberg tables are identified by catalog.namespace.table_name,
        # the 'filepath' parameter of SparkDataset is somewhat redundant here.
        # We can construct a placeholder or ensure it's provided by the user.
        # For simplicity, we'll ensure it's set to the fully qualified name for consistency.
        load_args = kwargs.get("load_args", {})
        self._catalog_name = load_args.get("catalog", "default")
        self._namespace = load_args.get("namespace", "default")
        self._table_name = load_args.get("table_name", "default")

        full_table_path = f"{self._catalog_name}.{self._namespace}.{self._table_name}"
        kwargs["filepath"] = full_table_path
        
        # Pass all remaining arguments to the base SparkDataset constructor.
        # This handles common settings like spark_session, save_args (if not explicitly handled by write_options), etc.
        super().__init__(**kwargs)

        self._data_validation = data_validation
        self._load_args = load_args
        self._save_args = kwargs.get("save_args", {})

        # Get parameters for dynamic filter
        self._range_date: Union[str, int] = self._load_args.get("range_date", "*")
        self._unit: str = self._load_args.get("unit", "day")
        self._partition_column: str = self._load_args.get("partition_column", "**")
        self._filters: Union[str, List[str]] = self._load_args.get("filters", "1=1")

        # Get information for saving table
        self._partitionedBy = self._save_args.get("partitionBy", [])

        try:
            self._processing_date = str(params["PROCESSING_DATE"])
        except KeyError:
            function_name = inspect.currentframe().f_code.co_name
            raise ValueError(f"[{function_name}] Missing required parameter: 'PROCESSING_DATE'")

        # Construct the fully qualified table name for Spark SQL operations
        self._full_iceberg_table_name = full_table_path

    def _load(self) -> DataFrame:
        """
        Loads data from the Iceberg table, applying filters if specified.
        The filters are pushed down to the source for efficient partition pruning.
        """
        spark = get_spark()

        # Start with the DataFrameReader, specifying "iceberg" format explicitly
        read_obj = spark.read.format("iceberg")

        # Load the Iceberg table. This returns a DataFrame.
        df = read_obj.load(self._full_iceberg_table_name, **self._load_args)

        # NOW, apply the filter on the DataFrame.
        # Spark's optimizer will attempt to push this down.
        # Determine the dynamic date filter
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


    def _save(self, df: DataFrame) -> None:
        """
        Saves a DataFrame to the Iceberg table using the Iceberg v2 API.
        Handles table creation, appending, and overwriting partitions.
        """

        if not isinstance(df, DataFrame):
            raise TypeError(
                f"Expected PySpark DataFrame, but got {type(df)}. "
                "SparkIcebergDataset only supports Spark DataFrames for saving."
            )

        writer = df.writeTo(self._full_iceberg_table_name)

        mode = self._save_args.get("mode", "overwrite")  # "append", "overwrite", etc.

        # Optional: handle partitioning when creating table
        if not self._exists():
            if self._partitionedBy:
                for col in self._partitionedBy:
                    writer = writer.partitionedBy(col)
            if mode == "append":
                writer.createOrReplace()
            else:
                writer.createOrReplace()
        else:
            if mode == "append":
                writer.append()
            elif mode == "overwrite":
                writer.overwritePartitions()
            else:
                raise ValueError(f"Unsupported mode '{mode}' for Iceberg write.")

    def _exists(self) -> bool:
        """Checks if the Iceberg table exists in the catalog."""

        spark = get_spark()
        
        try:
            full_table_name_with_catalog = f"{self._catalog_name}.{'.'.join(self._namespace) if isinstance(self._namespace, list) else self._namespace}.{self._table_name}"
            spark.sql(f"DESCRIBE TABLE {full_table_name_with_catalog}").collect()
            return True
        except Exception as e:
            error_message = str(e).lower()
            if "table or view not found" in error_message or "cannot be found" in error_message:
                self._logger.debug(f"Iceberg table '{self._full_iceberg_table_name}' does not exist.")
            else:
                self._logger.warning(
                    f"Could not reliably check existence of Iceberg table "
                    f"'{self._full_iceberg_table_name}': {e}"
                )
            return False

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._fs_prefix + str(self._filepath),
            "file_format": self._file_format,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
            "data_validation": self._data_validation,
            "type": "iceberg"
        }