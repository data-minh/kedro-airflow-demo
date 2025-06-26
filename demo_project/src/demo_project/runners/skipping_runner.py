import logging
from typing import Any, Dict, Tuple, List

from pluggy import PluginManager

from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.runner import SequentialRunner
from kedro.runner.runner import run_node
from ..helper_functions import check_data_completeness_auto
from omegaconf import OmegaConf


logger = logging.getLogger(__name__)

# Attempt to load global parameters from configuration file
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

def _parse_tags_to_dict_with_units(tags: List[str], prefix: str) -> Dict[str, Tuple[int, str]]:
    """
    Parses a list of tags and returns a dictionary where each value is a
    tuple containing the range and its corresponding time unit.

    - New format "prefix--name--number--unit" -> name: (number, unit)
    - Old format "prefix--name--range" -> name: (range, 'day')

    Args:
        tags: A list of strings to parse.
        prefix: The required prefix for a tag to be considered valid.

    Returns:
        A dictionary mapping the dataset name to a (range, unit) tuple.
        Example: {'raw_data': (7, 'day'), 'sales': (2, 'month')}
    """
    # Resulting dictionary to return
    parsed: Dict[str, Tuple[int, str]] = {}

    for tag in tags:
        parts = tag.split('--')
        
        try:
            # New tag format: "prefix--name--number--unit"
            if len(parts) == 4:
                tag_prefix, name, number_str, unit = parts
                
                if tag_prefix != prefix:
                    continue
                    
                range_value = int(number_str)
                if range_value <= 0:
                    continue
                
                # CHANGE 1: Assign value as a tuple (number, unit)
                parsed[name] = (range_value, unit)

            # Old tag format: "prefix--name--range"
            elif len(parts) == 3:
                tag_prefix, name, data_range = parts
                
                if tag_prefix != prefix:
                    continue
                    
                range_value = int(data_range)
                if range_value <= 0:
                    continue
                    
                # CHANGE 2: Assign value as a tuple with default unit 'day'
                parsed[name] = (range_value, 'day')

        except ValueError:
            # Safely skip invalid tags
            continue
            
    # CHANGE 3: Function now only returns the `parsed` dictionary
    return parsed


# def _parse_tag_ranges(tags: List[str], prefix: str, node_name: str) -> Dict[str, int]:
#     parsed = {}
#     for tag in tags:
#         try:
#             tag_prefix, name, data_range = tag.split('--')
#             if tag_prefix != prefix:
#                 continue
#             range_value = int(data_range)
#             if range_value <= 0:
#                 continue
#             parsed[name] = range_value
#         except ValueError:
#             continue
#     return parsed

def _resolve_dataset_path(dataset_config: dict) -> Tuple[str, str]:
    load_args = dict(dataset_config.get("load_args", {}))
    file_format = str(dataset_config.get("file_format", "...")).lower()

    if file_format == 'iceberg':
        catalog_name = load_args.get("catalog", "default")
        namespace = load_args.get("namespace", "default")
        table_name = load_args.get("table_name", "default")
        ns_str = ".".join([namespace] if isinstance(namespace, str) else namespace)
        path = f"{catalog_name}.{ns_str}.{table_name}"
    else:
        path = dataset_config.get("filepath")

    partition_column = str(load_args.get("partition_column", None))
    return path, partition_column


def _process_skip_node_checks_if_inputs_has_enough_data(node: Node, catalog: DataCatalog) -> Tuple[List[str], bool]:
    """
    Determine whether a node should be skipped based on the completeness of its inputs data.

    For each input dataset of the node:
    - Attempt to resolve its metadata and determine if the expected partitions for the given processing date are complete.
    - If all required data is available (i.e., no missing dates), the node is not marked as skippable.
    
    Returns:
        bool: True if the node should be skipped; otherwise, False.
    """

    inputs_data = _parse_tags_to_dict_with_units(node.tags, "input")
    processing_date = str(params["PROCESSING_DATE"])
    skipped_inputs = []

    for input_name in node.inputs:
        range_value, unit = inputs_data.get(input_name)
        if not isinstance(range_value, int):
            continue

        # Get information for dataset
        try:
            dataset_config = catalog._get_dataset(input_name)._describe()
        except Exception as e:
            logger.error(f"Failed to load catalog configuration: {e}")
            raise RuntimeError(f"Could not load dataset configuration from catalog configuration: {e}")
        
        # Get type of dataset
        type_of_dataset = dataset_config.get("type")
        if not type_of_dataset:
            raise ValueError("Missing 'type' in dataset configuration.")
        
        # Check to see the dataset needed data validation
        data_validation = dataset_config.get("data_validation", False)
        if not data_validation:
            continue
        
        try:
            path, partition_column = _resolve_dataset_path(dataset_config)
        except Exception as e:
            raise RuntimeError(f"Failed to get parameters of dataset {input_name}: {e}")
        
        logger.info(f"⏳ Validating dataset '{input_name}' at path '{path}'...")

        table_name, missing_dates = check_data_completeness_auto(
            path=path,
            processing_date=processing_date,
            range_date=range_value,
            partitionBy=partition_column,
            unit=unit,
            type_of_dataset=type_of_dataset
        )

        if not missing_dates:
            logger.info(
                f"✅ All expected data is available for table '{table_name}' at path: {path}"
            )
        else:
            error_message = (
                f"❌ Missing partitions for table '{table_name}' at path: {path}. "
                f"Missing dates: {missing_dates}"
            )
            logger.warning(error_message)
            skipped_inputs.append(input_name)

    if skipped_inputs:
        return skipped_inputs, True
    else:
        # Injected tags into dataset
        try:
            for input_name in node.inputs:
                dataset = catalog._get_dataset(input_name)
                range_value, unit = inputs_data.get(input_name, '*')
                dataset._range_date = range_value
                dataset._unit = unit
                logger.info(
                    f"📌 Injected tags {range_value} into dataset '{input_name}' "
                    f"from node '{node.name}'"
                )
        except Exception as e:
            logger.debug(f"Could not process dataset '{input_name}': {e}")

        return skipped_inputs, False
    
def _process_skip_node_when_output_already_has_data(node: Node, catalog: DataCatalog) -> bool:
    """
    Determine whether a node should be skipped based on the completeness of all its output data.
    Skip only if all outputs with valid range_date have complete data.
    """
    outputs_data = _parse_tags_to_dict_with_units(node.tags, "output")
    # If no output is valid → do not skip
    if not outputs_data:
        return False

    all_outputs_complete = True
    processing_date = str(params["PROCESSING_DATE"])

    for output_name in node.outputs:
        range_value, unit = outputs_data.get(output_name)
        if not isinstance(range_value, int):
            all_outputs_complete = False
            continue

        # Get information for dataset
        try:
            dataset_config = catalog._get_dataset(output_name)._describe()
        except Exception as e:
            logger.error(f"Failed to load catalog configuration: {e}")
            raise RuntimeError(f"Could not load dataset configuration from catalog configuration: {e}")
        
        # Get type of dataset
        type_of_dataset = dataset_config.get("type")
        if not type_of_dataset:
            raise ValueError("Missing 'type' in dataset configuration.")

        try:
            path, partition_column = _resolve_dataset_path(dataset_config)
        except Exception as e:
            raise RuntimeError(f"Failed to get parameters of dataset {output_name}: {e}")

        try:
            _, missing_dates = check_data_completeness_auto(
                path=path,
                processing_date=processing_date,
                range_date=range_value,
                partitionBy=partition_column,
                unit=unit,
                type_of_dataset=type_of_dataset
            )

            if missing_dates:
                all_outputs_complete = False

        except Exception as e:
            logger.warning(f"Node '{node.name}': Error checking output '{output_name}': {e}")
            all_outputs_complete = False

    return all_outputs_complete


class NodeSkippingRunner(SequentialRunner):
    """
    A custom Kedro runner that executes nodes sequentially and skips a node if
    its output data is already available and complete.

    Important characteristics:
    - Only the current node is skipped; downstream dependent nodes still execute.
    - Assumes that skipped nodes have their outputs materialized from previous successful runs.
    """

    def _run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager,
        session_id: str = None,
    ) -> None:
        """Run the pipeline with logic to skip nodes that already have complete outputs."""
        
        nodes = pipeline.nodes  
        
        for i, node in enumerate(nodes):
            logger.info(f"▶️   Starting node: '{node.name}'")
            if _process_skip_node_when_output_already_has_data(node, catalog):
                logger.info(
                    f"⏭️   Skipping execution for node: '{node.name}'. "
                )
                logger.info(f"Completed node {i + 1}/{len(nodes)}: '{node.name}'\n")
                continue
            
            input_list, flag = _process_skip_node_checks_if_inputs_has_enough_data(node, catalog)
            if flag:
                logger.warning(
                    f"⚠️ Skipping execution for node: '{node.name}' because lack of data at {input_list}\n"
                )
                continue
            
            try:
                run_node(node, catalog, hook_manager, self._is_async, session_id)
                logger.info(f"Completed node {i + 1}/{len(nodes)}: '{node.name}'\n")
            except Exception:
                logger.error(f"Node '{node.name}' has failed.", exc_info=True)
                raise
        
        logger.info("Pipeline execution complete.")

    def run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager = None,
        session_id: str = None,
    ) -> Dict[str, Any]:
        
        hook_manager = hook_manager or _NullPluginManager()
        catalog = catalog.shallow_copy()

        unsatisfied = pipeline.inputs() - set(catalog.list())
        if unsatisfied:
            raise ValueError(
                f"Pipeline input(s) {unsatisfied} not found in the DataCatalog"
            )
        self._run(pipeline, catalog, hook_manager, session_id)