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
                inputs="raw:4d",
                outputs="mutiple_intermediate_parquet_s3:4d",
                name="raw_to_intermediate_in_s3",
                tags=["test"]
            ),
            generate_node(
                func=load_and_sleep,
                inputs="mutiple_intermediate_parquet_s3:1m",
                outputs="mutiple_feature_parquet_s3:1m",
                name="intermediate_to_feature_in_s3",
                tags=["s3"]
            ),
            generate_node(
                func=transform_before_load_to_primary,
                inputs=["mutiple_feature_parquet_s3:1d", "mapping_primary:3d"],
                outputs="primary_icebreg_demo:1d",
                name="feature_in_s3_to_primary_in_iceberg",
                tags=["iceberg"]
            ),
        ]
    )