import os
import logging
import getpass
import yaml
from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pathlib import Path
import subprocess



logger = logging.getLogger(__name__)


# class SparkHooks:
#     @hook_impl
#     def after_context_created(self, context) -> None:
#         """Initialises a SparkSession using the config
#         defined in project's conf folder.
#         """
#         hadoop_home = "/usr/odp/1.2.4.0-102/hadoop"
#         classpath = subprocess.check_output([f'{hadoop_home}/bin/hadoop', 'classpath', '--glob'])
#         os.environ['CLASSPATH'] = classpath.decode('utf-8')
#         os.environ['ARROW_LIBHDFS_DIR'] = f"{hadoop_home}/lib/native/"

#         # Set HADOOP_CONF_DIR or YARN_CONF_DIR
#         hadoop_conf_dir = f"{hadoop_home}/etc/hadoop"
#         if os.path.exists(hadoop_conf_dir):
#             os.environ['HADOOP_CONF_DIR'] = hadoop_conf_dir
#             logger.info(f"Set HADOOP_CONF_DIR to {hadoop_conf_dir}")
#         else:
#             logger.warning(f"HADOOP_CONF_DIR {hadoop_conf_dir} does not exist. Please set it correctly.")

#         # Get the current user
#         current_user = getpass.getuser()
#         roles_file_path = Path("/path/to/user_roles.json")
#         if roles_file_path.exists():
#             with open(roles_file_path, 'r') as file:
#                 roles =  yaml.safe_load(file)
#             user_roles = "defaults"

#         else:
#             logger.warning(f"User roles file not found: {roles_file_path}")
#             user_roles =  ""
        
#         # Load the spark configuration in spark.yaml using the config loader
#         local_config_path = Path(f"/opt/spark-3.2.4-bin-hadoop3.2/conf/spark-{user_roles}.yml")
#         local_config = {}
#         if local_config_path.exists():
#             with open(local_config_path, 'r') as file:
#                 local_config = yaml.safe_load(file)

#         parameters = context.config_loader["spark"]
#         merged_config = {**parameters, **local_config}
#         logger.info("Final Spark configuration:")
#         for key, value in merged_config.items():
#             logger.info(f"  {key}: {value}")
#         spark_conf = SparkConf().setAll(merged_config.items())

#         # Initialise the spark session
#         spark_session_conf = (
#             SparkSession.builder.appName(context.project_path.name)
#             .config(conf=spark_conf)
#         )
#         _spark_session = spark_session_conf.getOrCreate()
#         _spark_session.sparkContext.setLogLevel("WARN")

class SparkHooks:

    @hook_impl
    def after_context_created(self, context) -> None:
        """Initialises a SparkSession using the config defined in spark.yaml."""
        parameters = context.config_loader["spark"]
        spark_conf = SparkConf().setAll(parameters.items())
        spark_session_conf = (
            SparkSession.builder
            .appName(context.project_path.name)
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")