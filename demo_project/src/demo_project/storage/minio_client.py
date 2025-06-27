from minio import Minio
import logging


class MinIOClient():
    def __init__(self, aws_access_key_id, aws_secret_access_key, endpoint_url, secure):
        # Set up our logger
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger()

        try:
            self.client = Minio(
                endpoint=endpoint_url,
                access_key=aws_access_key_id,
                secret_key=aws_secret_access_key,
                secure=secure
        )
        except Exception as err:
            self.logger.info("An exception: %s", err)
            raise err
        
        self.logger.info("Server Conneted to Ceph: %s", endpoint_url)

    def list_objects(self, bucket, prefix, recursive=False):
        objects = self.client.list_objects(bucket, prefix=prefix, recursive=False)
        return objects

    def check_s3_exist(self, bucket_name, folder_path):
        """
        Kiểm tra xem một 'folder' (prefix) có tồn tại trong bucket hay không.
        """
        try:
            # Check bucket exist
            if not self.client.bucket_exists(bucket_name):
                self.logger.warning("Bucket '%s' does not exist.", bucket_name)
                return False

            # Check object in folder
            objects = self.client.list_objects(bucket_name, prefix=folder_path, recursive=False)
            for obj in objects:
                return True
            return False
        except Exception as err:
            self.logger.error("Error checking folder existence in bucket '%s': %s", bucket_name, err)
            raise

