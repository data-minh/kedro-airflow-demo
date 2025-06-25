import os
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Set
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import pyarrow as pa
import pyarrow.fs as pafs
from dotenv import load_dotenv

try:
    from minio import Minio
    from minio.error import S3Error
except:
    pass


logger = logging.getLogger(__name__)

def get_spark() -> SparkSession:
    """
    Returns the SparkSession.
    """
    spark = SparkSession.builder.getOrCreate()

    return spark

from datetime import datetime, timedelta
from typing import List
from dateutil.relativedelta import relativedelta

def get_date_range_for_folder_pattern(processing_date: str, range_date: int, unit: str) -> List[str]:
    """
    Generates a list of specific date strings based on a unit.
    - For 'day': returns a continuous list of dates.
    - For 'month', 'quarter', 'year': returns a discrete list of the last day of each period in the range.

    Args:
        processing_date (str): Date in 'yyyy-mm-dd' format.
        range_date (int): Number of units to include in the range.
        unit (str): The time unit, one of ['day', 'month', 'quarter', 'year'].

    Returns:
        List[str]: List of date strings based on the specified logic.
    """
    # --- 1. Input Validation ---
    try:
        current_date = datetime.strptime(processing_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid processing_date format: '{processing_date}'. Expected 'YYYY-MM-DD'.")

    if not isinstance(range_date, int) or range_date <= 0:
        raise ValueError(f"Invalid range_date: {range_date}. It must be a positive integer.")
        
    valid_units = ['day', 'month', 'quarter', 'year']
    if unit not in valid_units:
        raise ValueError(f"Invalid unit: '{unit}'. Must be one of {valid_units}.")

    # --- 2. Logic based on unit ---
    
    # 'day' case has a separate logic and returns immediately
    if unit == 'day':
        start_date = current_date - timedelta(days=range_date - 1)
        return [
            (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
            for i in range(range_date)
        ]

    # 'month', 'quarter', and 'year' share the logic of generating period-end dates
    dates_to_return = []
    for i in range(range_date):
        if unit == 'month':
            # Get a date in the target month (shifted back by i months)
            target_date = current_date + relativedelta(months=-i)
            # Get the last day of that month
            period_end = target_date + relativedelta(day=31)
            dates_to_return.append(period_end.strftime('%Y-%m-%d'))

        elif unit == 'quarter':
            # Get a date in the target quarter (shifted back by i quarters, 1 quarter = 3 months)
            target_date = current_date + relativedelta(months=-(i * 3))
            # Get the last day of that quarter
            quarter = (target_date.month - 1) // 3 + 1
            end_month_of_quarter = quarter * 3
            period_end = target_date + relativedelta(month=end_month_of_quarter, day=31)
            dates_to_return.append(period_end.strftime('%Y-%m-%d'))
            
        elif unit == 'year':
            # Get a date in the target year (shifted back by i years)
            target_date = current_date + relativedelta(years=-i)
            # The last day of that year is always Dec 31
            period_end = datetime(target_date.year, 12, 31)
            dates_to_return.append(period_end.strftime('%Y-%m-%d'))

    return dates_to_return



def get_date_range(processing_date: str, range_date: int, unit: str) -> List[str]:
    """
    Generate a list of date strings based on a processing date, a range, and a time unit.

    - For 'day', it generates a range of `range_date` days ending on `processing_date`.
    - For 'month', 'quarter', 'year', it determines the period's end date based on `processing_date`
      and the period's start date by looking back `range_date` units.

    Args:
        processing_date (str): Date in 'yyyy-mm-dd' format.
        range_date (int): Number of units (days, months, etc.) for the range.
        unit (str): The time unit, one of ['day', 'month', 'quarter', 'year'].

    Returns:
        List[str]: A list of date strings from start_date to end_date.
        
    Raises:
        ValueError: If inputs are invalid.
    """
    # --- 1. Input Validation ---
    try:
        current_date = datetime.strptime(processing_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid processing_date format: '{processing_date}'. Expected 'YYYY-MM-DD'.")

    if not isinstance(range_date, int) or range_date <= 0:
        raise ValueError(f"Invalid range_date: {range_date}. It must be a positive integer.")
        
    valid_units = ['day', 'month', 'quarter', 'year']
    if unit not in valid_units:
        raise ValueError(f"Invalid unit: '{unit}'. Must be one of {valid_units}.")

    start_date = None
    end_date = None

    # --- 2. Logic to determine start_date and end_date based on unit ---
    if unit == 'day':
        # Previous logic: end_date is the processing_date, start_date is (range_date - 1) days before
        end_date = current_date
        start_date = end_date - timedelta(days=range_date - 1)

    elif unit == 'month':
        # end_date: the last day of the month containing processing_date
        end_date = current_date + relativedelta(day=31)
        # start_date: the first day of the month (range_date - 1) months ago
        start_date = current_date + relativedelta(months=-(range_date - 1), day=1)

    elif unit == 'quarter':
        # end_date: last day of the quarter containing processing_date
        current_quarter = (current_date.month - 1) // 3 + 1
        end_of_quarter_month = current_quarter * 3
        end_date = current_date + relativedelta(month=end_of_quarter_month, day=31)
        
        # start_date: first day of the quarter (range_date - 1) quarters ago
        # Find the first day of the current quarter
        first_month_of_current_quarter = (current_quarter - 1) * 3 + 1
        first_day_of_current_quarter = datetime(current_date.year, first_month_of_current_quarter, 1)
        # Go back N quarters (1 quarter = 3 months)
        start_date = first_day_of_current_quarter + relativedelta(months=-((range_date - 1) * 3))

    elif unit == 'year':
        # end_date: last day of the year containing processing_date (Dec 31)
        end_date = datetime(current_date.year, 12, 31)
        # start_date: first day of the year (range_date - 1) years ago (Jan 1)
        start_date = datetime(current_date.year, 1, 1) + relativedelta(years=-(range_date - 1))

    # --- 3. Generate the list of dates from start_date to end_date ---
    if start_date and end_date:
        # Calculate total number of days to generate
        total_days = (end_date - start_date).days + 1
        return [
            (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
            for i in range(total_days)
        ]
    
    return []


def get_partitioned_dates_local(table_path: str, partition_column: str = None) -> List[str]:
    """
    List all partition folders in a local directory in the format partition_column=yyyy-mm-dd.

    Args:
        table_path (str): Local path to the partitioned table.
        partition_column (str): The name of the partition column (e.g., 'event_date').

    Returns:
        List[str]: List of date strings found in the partitions.
    """
    date_strings = set()
    if not os.path.isdir(table_path):
        return []

    for entry_name in os.listdir(table_path):
        full_path = os.path.join(table_path, entry_name)

        if os.path.isdir(full_path):
            date_candidate = None
            
            if partition_column and partition_column.lower() != 'none':
                prefix = f"{partition_column}="
                if entry_name.startswith(prefix):
                    date_candidate = entry_name.split('=')[1]
            
            else:
                date_candidate = entry_name

            if date_candidate:
                try:
                    datetime.strptime(date_candidate, '%Y-%m-%d')
                    date_strings.add(date_candidate)
                except ValueError:
                    pass
    
    return sorted(list(date_strings))


def get_partitioned_dates_s3(bucket: str, prefix: str, partition_column: str) -> List[str]:
    """
    List all partition folders in an S3 prefix in the format partition_column=yyyy-mm-dd.

    Args:
        bucket (str): S3 bucket name.
        prefix (str): S3 prefix (folder path inside the bucket).
        partition_column (str): The name of the partition column (e.g., 'event_date').

    Returns:
        List[str]: List of date strings found in the partitions.
    """
    load_dotenv()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    endpoint = os.getenv("AWS_S3_ENDPOINT").replace("http://", "").replace("https://", "")
    secure = os.getenv("AWS_S3_ENDPOINT").startswith("https://")

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

    date_strings = set()

    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=False)
        for obj in objects:
            if obj.is_dir:
                entry_name: str = os.path.basename(os.path.normpath(obj.object_name))
                date_candidate = None

                if partition_column and partition_column.lower() != 'none':
                    prefix_check = f"{partition_column}="
                    if entry_name.startswith(prefix_check):
                        date_candidate = entry_name.split('=')[1]
                else:
                    date_candidate = entry_name

                if date_candidate:
                    try:
                        datetime.strptime(date_candidate, '%Y-%m-%d')
                        date_strings.add(date_candidate)
                    except ValueError:
                        pass

    except S3Error as e:
        logger.warning(f"ERROR S3: {e}")
        return []

    return sorted(list(date_strings))


# def _init_hdfs(host='localhost', port=8020, user='root'):
#     import os
#     import subprocess
#     from dotenv import load_dotenv

#     load_dotenv()
#     hadoop_home = os.getenv('HADOOP_HOME')
#     java_home = os.getenv('JAVA_HOME')

#     if not hadoop_home or not os.path.exists(hadoop_home):
#         raise EnvironmentError("HADOOP_HOME is not set or the path does not exist.")

#     if java_home and os.path.exists(java_home):
#         os.environ['JAVA_HOME'] = java_home

#     try:
#         classpath_cmd = os.path.join(hadoop_home, 'bin', 'hdfs') + ' classpath --glob'
#         result = subprocess.run(
#             classpath_cmd, shell=True, capture_output=True, text=True, check=True
#         )
#         os.environ['HADOOP_HOME'] = hadoop_home
#         os.environ['CLASSPATH'] = result.stdout.strip()
#     except subprocess.CalledProcessError as e:
#         raise RuntimeError(f"Error while retrieving HDFS classpath: {e.stderr.strip()}") from e
#     except FileNotFoundError:
#         raise FileNotFoundError(f"'hdfs' command not found at {os.path.join(hadoop_home, 'bin')}")

#     try:
#         hdfs = pafs.HadoopFileSystem(host=host, port=port, user=user)
#         return hdfs
#     except Exception as e:
#         raise RuntimeError(f"Failed to initialize PyArrow HDFS: {e}") from e


def get_partitioned_dates_hdfs(url: str, path: str, partition_column: str = "") -> List[str]:
    """
    List all partition folders in HDFS under the given path.
    Partitions can be in the format partition_column=yyyy-mm-dd or just yyyy-mm-dd.

    Args:
        path (str): Root path in HDFS (e.g., '/warehouse/events/')
        partition_column (str): Name of the partition column (e.g., 'event_date')

    Returns:
        List[str]: A list of valid date strings in 'yyyy-mm-dd' format
    """

    fs = pa.fs.HadoopFileSystem.from_uri(f"hdfs://{url}")  
    # fs = _init_hdfs()
    selector = pa.fs.FileSelector(path, allow_not_found=True, recursive=False)
    date_strings = set()

    try:
        infos = fs.get_file_info(selector)
        for info in infos:
            if info.type == pa.fs.FileType.Directory:
                entry_name = info.base_name
                date_candidate = None
                if partition_column and partition_column.lower() != 'none':
                    prefix_check = f"{partition_column}="
                    if entry_name.startswith(prefix_check):
                        date_candidate = entry_name.split('=')[1]
                else:
                    date_candidate = entry_name
                if date_candidate:
                    try:
                        datetime.strptime(date_candidate, '%Y-%m-%d')
                        date_strings.add(date_candidate)
                    except ValueError:
                        pass
    except Exception as e:
        logger.debug(f"Error reading from HDFS: {e}")
        return []

    return sorted(list(date_strings))
    


def get_partitioned_dates_iceberg(path: str, partitionBy: str) -> Set[str]:
    spark = get_spark()

    query = f"""
    SELECT file
    FROM {path}.metadata_log_entries
    ORDER BY timestamp DESC
    LIMIT 1
    """

    try:
        result = spark.sql(query).collect()
    except:
        return []

    if not result:
        raise ValueError(f"No metadata file found for table '{path}'")

    current_metadata_file_location = result[0]["file"]
    metadata_content_df = spark.read.option("multiLine", "true").json(current_metadata_file_location)
    current_snapshot_id = metadata_content_df.select("current-snapshot-id").collect()[0][0]
    current_snapshot_df = metadata_content_df.select(explode("snapshots").alias("snapshot")) \
                                            .where(col("snapshot.snapshot-id") == current_snapshot_id)
    manifest_list_path = current_snapshot_df.select("snapshot.manifest-list").collect()[0][0]
    manifest_list_data = spark.read.format("avro").load(manifest_list_path)
    manifest_paths = [row.manifest_path for row in manifest_list_data.select("manifest_path").collect()]

    if not manifest_paths:
        logger.warning(f"Warning: No manifest file found in {path}")
        return []
    else:
        all_manifest_entries_df = spark.read.format("avro").load(manifest_paths)

        inserted_files_df = all_manifest_entries_df.where((col("data_file.content") == 0) & (col("status").isin(0,1))).select(
            col("data_file.file_path").alias("inserted_file_path"),
            col(f"data_file.partition.{partitionBy}").alias("partition_date")
        ).distinct()

        delete_files_df = all_manifest_entries_df.where(col("data_file.content") != 0).select(
            col("data_file.file_path").alias("delete_file_path")
        ).distinct()
        
        delete_file_paths = [row.delete_file_path for row in delete_files_df.select("delete_file_path").collect()]

        paths_to_remove_df_with_status_is_2 = all_manifest_entries_df.where(col("status")==2).select(
            col("data_file.file_path").alias("path_to_remove")
        ).distinct()

        if not delete_file_paths:
            final_paths_to_remove_df = paths_to_remove_df_with_status_is_2
        else:
            all_deleted_records_df = spark.read.parquet(*delete_file_paths)
            
            paths_to_remove_df = all_deleted_records_df.where(col("pos") == 0).select(
                col("file_path").alias("path_to_remove")
            ).distinct()

            final_paths_to_remove_df = paths_to_remove_df_with_status_is_2.union(paths_to_remove_df).distinct()

        final_live_files_df = inserted_files_df.join(
            final_paths_to_remove_df,
            inserted_files_df.inserted_file_path == final_paths_to_remove_df.path_to_remove,
            "left_anti"
        )

        final_partition_dates_df = final_live_files_df.select("partition_date").distinct()
        partition_dates = {str(row['partition_date']) for row in final_partition_dates_df.collect()}

        return partition_dates


def check_data_completeness_auto(path: str, processing_date: str, range_date: int, partitionBy: str, unit: str, type_of_dataset: str) -> tuple[str, List[str]]:
    """
    Automatically determine the storage type and check for completeness of partitioned data
    for a given table path and date range.

    Args:
        path (str): Table path, can be local, s3://bucket/prefix, hdfs://path or iceberg table with "catalog.schema.table_name"
        processing_date (str): The latest date to be included (yyyy-mm-dd).
        range_date (int): Number of days to go back, including processing_date.
        partitionBy (str): The name of the partition column (e.g., 'event_date').

    Returns:
        tuple[str, List[str]]: A tuple containing the table name and a list of missing date strings.
    """
    parsed = urlparse(path)
    table_name = os.path.basename(parsed.path.rstrip('/')) if parsed.path else parsed.netloc

    actual_dates: List[str] = []

    # Determine storage type and get actual dates
    if parsed.scheme == 's3' or parsed.scheme == 's3a':
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/') + '/'
        actual_dates = get_partitioned_dates_s3(bucket, prefix, partitionBy)
    elif parsed.scheme == 'hdfs':
        actual_dates = get_partitioned_dates_hdfs(parsed.netloc, parsed.path, partitionBy)
    elif (parsed.scheme == '' and '/' in path) or parsed.scheme == 'file': 
        actual_dates = get_partitioned_dates_local(parsed.path, partitionBy)
    elif parsed.scheme == '' and len(path.split('.')) == 3:
        actual_dates = get_partitioned_dates_iceberg(parsed.path, partitionBy)
    else:
        logger.error(f"Unsupported path scheme: '{parsed.scheme}' for path: {path}")
        return table_name, get_date_range(processing_date, range_date)

    expected_dates: List[str] = []
    # Generate expected date range
    if type_of_dataset == 'folder_pattern':
        expected_dates = get_date_range_for_folder_pattern(processing_date=processing_date, range_date=range_date, unit=unit)
    else:
        expected_dates = get_date_range(processing_date=processing_date, range_date=range_date, unit=unit)
    missing_dates = [d for d in expected_dates if d not in actual_dates]

    return table_name, missing_dates


###################################################################################
################################### GENERATE NODE #################################
###################################################################################

# Update import to include Union and Tuple
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, Tuple 
from kedro.pipeline import node


# # Correct the type hint for io_data and the return value
# def _parse_io_and_generate_tags(
#     io_data: Union[str, List[str], Dict[str, str]], prefix: str
# ) -> Tuple[Union[str, List[str], Dict[str, str]], List[str]]:
#     """
#     Internal helper function to parse inputs or outputs, split the dataset name, and create tags.
#     Example: "raw_data:7" -> ("raw_data", ["input--raw_data--7"])
#     """
#     if io_data is None:
#         return None, []

#     # Normalize to a list for processing
#     io_list = [io_data] if isinstance(io_data, str) else io_data
    
#     cleaned_names = []
#     generated_tags = []

#     for item in io_list:
#         if ":" in item:
#             parts = item.split(":", 1)
#             name = parts[0]
#             if len(parts) == 1:
#                 weight = 0
#             else:
#                 weight = parts[1]
#                 if weight == '*':
#                     weight = 0
#             cleaned_names.append(name)
#             generated_tags.append(f"{prefix}--{name}--{weight}")
#         else:
#             # If there is no weight, just take the dataset name
#             cleaned_names.append(item)
            
#     # Return the original data type (string if input was string, list if it was a list)
#     final_cleaned_names = cleaned_names[0] if isinstance(io_data, str) else cleaned_names
    
#     return final_cleaned_names, generated_tags

def _parse_io_and_generate_tags(
    io_data: Union[str, List[str], Dict[str, str]], prefix: str
) -> Tuple[Union[str, List[str], Dict[str, str]], List[str]]:
    """
    Internal helper function to parse inputs or outputs, split the dataset name, and create tags.
    
    New format examples:
    - "data:4d" -> ("data", ["input--data--4--day"])
    - "data:4m" -> ("data", ["input--data--4--month"])
    - "data:4q" -> ("data", ["input--data--4--quarter"])
    - "data:4y" -> ("data", ["input--data--4--year"])
    
    Old format example:
    - "raw_data:7" -> ("raw_data", ["input--raw_data--7"])
    """
    if io_data is None:
        return None, []

    # Normalize to a list for processing. If it's a dict, we process the keys.
    io_list = io_data
    if isinstance(io_data, str):
        io_list = [io_data]
    elif isinstance(io_data, dict):
        io_list = list(io_data.keys())
        
    cleaned_names = []
    generated_tags = []

    # Map for time units
    time_unit_map = {
        'd': 'day',
        'm': 'month',
        'q': 'quarter',
        'y': 'year'
    }

    for item in io_list:
        if ":" in item:
            name, weight_part = item.split(":", 1)
            cleaned_names.append(name)

            # Check for new format with time units (e.g., "4d", "12m")
            if (len(weight_part) > 1 and 
                    weight_part[:-1].isdigit() and 
                    weight_part[-1].lower() in time_unit_map):
                
                number = weight_part[:-1]
                unit_key = weight_part[-1].lower()
                unit_full_name = time_unit_map[unit_key]
                generated_tags.append(f"{prefix}--{name}--{number}--{unit_full_name}")
            
            else:
                # Fallback to old logic for ":7", ":*", or just ":"
                weight = weight_part
                if weight == '*' or not weight: # Handle wildcard or empty string after ":"
                    weight = '0'
                generated_tags.append(f"{prefix}--{name}--{weight}")
        else:
            # If there is no weight, just take the dataset name
            cleaned_names.append(item)
            
    # Return the original data type (string if input was string, list if it was a list)
    final_cleaned_names = cleaned_names
    if isinstance(io_data, str):
        final_cleaned_names = cleaned_names[0]
    elif isinstance(io_data, dict):
        # For dict, we return a list of cleaned names as keys were processed
        final_cleaned_names = cleaned_names
    
    return final_cleaned_names, generated_tags


# Correct the type hints for inputs and outputs
def generate_node(
    func: Callable,
    inputs: Union[str, List[str], Dict[str, str]],
    outputs: Union[str, List[str], Dict[str, str]],
    *,
    name: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
    **kwargs: Any,
) -> node:
    """
    Creates a Kedro node with the ability to auto-generate tags from inputs and outputs.
    Syntax: "dataset_name:weight"
    """
    # Parse inputs and outputs to get clean names and tags
    cleaned_inputs, input_tags = _parse_io_and_generate_tags(inputs, "input")
    cleaned_outputs, output_tags = _parse_io_and_generate_tags(outputs, "output")

    # Combine the generated tags with user-provided tags
    all_tags = input_tags + output_tags
    if tags:
        all_tags.extend(tags)
        
    # Call the original Kedro node function with the processed parameters
    return node(
        func=func,
        inputs=cleaned_inputs,
        outputs=cleaned_outputs,
        name=name,
        tags=all_tags or None, # Return None if the list is empty
        **kwargs
    )
