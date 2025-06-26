from .connector import PostgresConnector
from ..configs import Settings


db_connector = PostgresConnector(
    host=Settings.POSTGRES_HOST,
    port=Settings.POSTGRES_PORT,
    database=Settings.POSTGRES_DB,
    user=Settings.POSTGRES_USER,
    password=Settings.POSTGRES_PASSWORD
)
