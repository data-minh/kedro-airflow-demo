from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    IS_DB_LOGGING: bool
    IS_AIRFLOW_RUN: bool
    
    AWS_S3_ENDPOINT: str
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str
    AWS_S3_BUCKET: str

    AWS_DEFAULT_REGION: str
    MINIO_REGION: str

    WAREHOUSE: str
    NESSIE_URI: str

    POSTGRES_HOST: str
    POSTGRES_PORT: str|int
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str

    class Config:
        env_file = ".env"