from typing import Any
import lazy_loader as lazy


FilteredSparkDataset: Any
IcebergSparkDataset: Any
FolderPatternSparkDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "filtered_spark_dataset": ["FilteredSparkDataset"],
        "iceberg_spark_dataset": ["IcebergSparkDataset"],
        "folder_pattern_spark_dataset": ["FolderPatternSparkDataset"],
    },
)