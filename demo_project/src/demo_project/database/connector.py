import os
from typing import Union, List
import logging
import psycopg2

from ..data_model import Node, Pipeline  # Bạn giữ nguyên Node & Pipeline
from ..configs import Settings

logger = logging.getLogger(__name__)

class ObjectSerializer:
    @staticmethod
    def serialize_node(node: Node) -> dict:
        return {
            "ID": node.ID,
            "run_id": node.run_id,
            "run_date": node.run_date,
            "start_time": node.start_time,
            "finish_time": node.finish_time,
            "func": node.func if isinstance(node.func, str) else node.func.__name__,
            "inputs": list(node.inputs) if isinstance(node.inputs, (list, dict)) else [node.inputs] if node.inputs else [],
            "outputs": list(node.outputs) if isinstance(node.outputs, (list, dict)) else [node.outputs] if node.outputs else [],
            "name": node.name,
            "tags": list(node.tags) if isinstance(node.tags, (list, set, tuple)) else [node.tags] if node.tags else [],
            "confirms": list(node.confirms) if isinstance(node.confirms, list) else [node.confirms] if node.confirms else [],
            "namespace": node.namespace,
            "status": node.status.value,
            "detail": node.detail,
        }

    @staticmethod
    def serialize_pipeline(pipeline: Pipeline) -> dict:
        return {
            "ID": pipeline.ID,
            "dag_name": pipeline.dag_name,
            "run_id": pipeline.run_id,
            "run_date": pipeline.run_date,
            "start_time": pipeline.start_time,
            "finish_time": pipeline.finish_time,
            "nodes": pipeline.nodes,
            "inputs": list(pipeline.inputs) if isinstance(pipeline.inputs, (list, dict)) else [pipeline.inputs] if pipeline.inputs else [],
            "outputs": list(pipeline.outputs) if isinstance(pipeline.outputs, (list, dict)) else [pipeline.outputs] if pipeline.outputs else [],
            "parameters": list(pipeline.parameters) if pipeline.parameters else [],
            "tags": list(pipeline.tags) if isinstance(pipeline.tags, (list, set, tuple)) else [pipeline.tags] if pipeline.tags else [],
            "namespace": pipeline.namespace,
        }

    @staticmethod
    def serialize_nodes_from_pipeline(pipeline: Pipeline) -> List[dict]:
        nodes = pipeline.nodes if isinstance(pipeline.nodes, list | tuple) else [pipeline.nodes]
        return [ObjectSerializer.serialize_node(n) for n in nodes]


class PostgresConnector:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ):
        self.conn = None
        self.is_db_logging = Settings.IS_DB_LOGGING
        self.is_airflow_run = Settings.IS_AIRFLOW_RUN
        
        if self.is_db_logging:
            self.conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=database,
            )
            self.conn.autocommit = True

    def get_max_pipeline_id(self) -> int | None:
        if not self.is_db_logging:
            return None
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT MAX(ID) FROM pipelines")
            result = cursor.fetchone()
            return result[0] if result and result[0] is not None else 0
        except Exception as e:
            logger.error(f"Error: {e}")
            raise


    def insert_or_update_pipeline(self, pipeline: Pipeline) -> Pipeline:
        if not self.is_db_logging:
            return pipeline
        if self.is_airflow_run:
            pipeline.run_id = os.environ.get('AIRFLOW_CTX_DAG_RUN_ID')
            pipeline.dag_name = os.environ.get('AIRFLOW_CTX_DAG_ID')
            
        cursor = self.conn.cursor()
        data = ObjectSerializer.serialize_pipeline(pipeline)

        columns = [k for k in data.keys() if k != "ID" or data["ID"]]
        values = [data[k] for k in columns]

        insert_cols = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns])

        query = f"""
            INSERT INTO pipelines ({insert_cols})
            VALUES ({placeholders})
            ON CONFLICT (ID) DO UPDATE SET {update_clause}
            RETURNING ID
        """
        try:
            cursor.execute(query, values)
            returned_id = cursor.fetchone()[0]
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Insert/update pipeline failed: {e}")

        # Gán lại ID vào object nếu nó chưa có
        if not hasattr(pipeline, 'ID') or pipeline.ID is None:
            pipeline.ID = returned_id

        # self.insert_or_update_nodes(pipeline)
        return pipeline


    def insert_or_update_nodes(self, pipeline: Pipeline) -> None:
        if not self.is_db_logging:
            return None
        if self.is_airflow_run:
            pipeline.run_id = os.environ.get('AIRFLOW_CTX_DAG_RUN_ID')

        try:
            cursor = self.conn.cursor()
            nodes_data = ObjectSerializer.serialize_nodes_from_pipeline(pipeline)

            for node in nodes_data:
                node["pipeline_id"] = pipeline.ID  # Nếu bạn có cột pipeline_id (nếu không có thì bỏ dòng này)

                columns = list(node.keys())
                values = [node[col] for col in columns]

                insert_columns = ', '.join(columns)
                insert_placeholders = ', '.join(['%s'] * len(columns))

                # Lấy phần update cho ON CONFLICT
                update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in ('ID', 'name')])

                query = f"""
                    INSERT INTO nodes ({insert_columns})
                    VALUES ({insert_placeholders})
                    ON CONFLICT (ID, name) DO UPDATE SET {update_clause}
                """
                cursor.execute(query, values)

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Lỗi khi insert/update nodes: {e}")


    def insert_or_update_node(self, node: Node):
        if not self.is_db_logging:
            return None
        
        if self.is_airflow_run:
            node.run_id = os.environ.get('AIRFLOW_CTX_DAG_RUN_ID')

        cursor = self.conn.cursor()

        data = ObjectSerializer.serialize_node(node)

        # Columns & values
        columns = list(data.keys())
        values = list(data.values())

        # Build INSERT & UPDATE clause
        insert_columns = ', '.join(columns)
        insert_placeholders = ', '.join(['%s'] * len(values))
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns])

        # Conflict target: tổ hợp duy nhất bạn chọn
        query = f"""
            INSERT INTO nodes ({insert_columns})
            VALUES ({insert_placeholders})
            ON CONFLICT (ID, name) DO UPDATE SET {update_clause}
        """

        try:
            cursor.execute(query, values)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"Insert/update Node failed: {e}")


    def close(self):
        self.conn.close()
