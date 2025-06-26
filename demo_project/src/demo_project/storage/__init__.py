from .minio_client import MinIOClient
from ..configs import Settings


minio_client = MinIOClient(
    endpoint_url=Settings.AWS_S3_ENDPOINT.replace("http://", "").replace("https://", ""),
    aws_access_key_id=Settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=Settings.AWS_SECRET_ACCESS_KEY,
    secure=Settings.AWS_S3_ENDPOINT.startswith("https://")
)
