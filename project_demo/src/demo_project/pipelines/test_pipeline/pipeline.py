import logging
from kedro.pipeline import Pipeline, node, pipeline
from ...helper_functions import generate_node
from .nodes import load_and_sleep, transform_before_load_to_primary, transform_before_load_to_feature

logger = logging.getLogger(__name__)

def create_pipeline(**kwargs) -> Pipeline:
    """Creates the project's pipeline."""
    return pipeline(
        [
            generate_node(
                func=load_and_sleep,
                inputs="raw:1d",
                outputs="mutiple_intermediate_parquet_hadoop:1d",
                name="raw_to_intermediate_in_hadoop",
                tags=["test"]
            ),
            generate_node(
                func=load_and_sleep,
                inputs="mutiple_intermediate_parquet_hadoop:1d",
                outputs="mutiple_feature_parquet_hadoop:1d",
                name="intermediate_to_feature_in_hadoop",
                tags=["hadoop"]
            ),
            generate_node(
                func=load_and_sleep,
                inputs="mutiple_feature_parquet_hadoop:1d",
                outputs="primary_icebreg:1d",
                name="feature_to_primary_in_iceberg",
                tags=["iceberg"]
            ),
        ]
    )