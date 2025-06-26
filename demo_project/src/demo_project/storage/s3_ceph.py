
import boto3
import logging
import hashlib
import time
import os
import enum
import datetime


class FileStatus(enum.Enum):
    NEW = 0
    UPLOADED = 1
    REJECTED = 2

class TimeUnit(enum.Enum):
    DAYS = 60*60*24
    HOURS = 60*60
    MINUTES = 60
    SECONDS = 1

class S3FileDescription:
    def __init__(self, status, bucket_name, file_name, file_size, file_extension, file_url, file_url_expiration_date, file_check_sum):
        self.status = status
        self.bucketName = bucket_name
        self.fileName = file_name
        self.fileSize = file_size
        self.fileExtension = file_extension
        self.fileURL = file_url
        self.fileUrlExpriationDate = file_url_expiration_date
        self.fileCheckSum = file_check_sum

    def toString(self):
        return f"status : {self.status} - bucketName : {self.bucketName} - fileName : {self.fileName} \
         -  fileSize : {self.fileSize}  - fileExtension : {self.fileExtension} \
         - fileURL : {self.fileURL} \
         - fileUrlExpriationDate : {self.fileUrlExpriationDate} - fileCheckSum {self.fileCheckSum} "

def current_milli_time():
    return round(time.time() * 1000)

def getExpirationDateFromCurrentTime(expired_duration, time_unit):
    current =  current_milli_time()
    # print(current)
    expired_time = current + (expired_duration * time_unit.value * 1000)
    # print(expired_time)

    expired_date = datetime.datetime.fromtimestamp(expired_time/1000)
    return expired_date

class S3Ceph():
    def __init__(self, aws_access_key_id, aws_secret_access_key, endpoint_url):
        # Set up our logger
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger()

        try:
            session = boto3.session.Session()
            self.s3_client = session.client(
                service_name='s3',
                aws_access_key_id= aws_access_key_id,
                aws_secret_access_key= aws_secret_access_key,
                endpoint_url= endpoint_url
            )
        except Exception as err:
            self.logger.info("An exception: %s", err)
            raise err
        
        self.logger.info("Server Conneted to Ceph: %s", endpoint_url)
            
    def get_list_bucket(self):
        list_bucket = [bucket['Name'] for bucket in self.s3_client.list_buckets()["Buckets"]]
        return list_bucket
    
    def update_list_bucket(self):
        self.list_bucket = self.get_list_bucket()

    def create_bucket(self, bucket_name):
        try:
            self.s3_client.create_bucket(Bucket=bucket_name)
            self.update_list_bucket()
        except Exception as  err:
            self.logger.error("Create Bucket %s Error: %s", bucket_name, err)
            return False
        return True
    
    def check_path_exist(self, path):
        # example: "s3://vss/pq/ls" split -> ['s3:', '', 'vss', 'pq', 'ls'] -> bucket name = vss
        bucket_name = path.split("/")[2]
        prefix = None
        
        if f"s3://{bucket_name}" in path:
            prefix = path.replace(f"s3://{bucket_name}",'')
        count = self.s3_client.list_objects(Bucket=bucket_name,
                                          Prefix=prefix)
        
        if 'Contents' in count.keys():
            return True
        else:
            return False
        
    def delete_folder(self, path):
        # example: "s3://vss/pq/ls" split -> ['s3:', '', 'vss', 'pq', 'ls'] -> bucket name = vss
        bucket_name = path.split("/")[2]
        prefix = None
        
        if f"s3://{bucket_name}" in path:
            prefix = path.replace(f"s3://{bucket_name}",'')
            
        if not self.check_path_exist(path=path):
            self.logger.info("Delete folder path not existed: %s", path)
            return True
        try:
            # Delete using "remove_object"
            objects_to_delete = self.s3_client.list_objects(bucket_name, prefix=prefix, recursive=True)
            for obj in objects_to_delete:
                self.s3_client.remove_object(bucket_name, obj.object_name)
            return True
        except Exception as  err:
            self.logger.error("Delete folder %s Error: %s", path, err)
            return False
        
    def update_path(self, path, path_update):
        # example: "s3://vss/pq/ls" split -> ['s3:', '', 'vss', 'pq', 'ls'] -> bucket name = vss
        bucket_name = path.split("/")[2]
        prefix = None
        
        if f"s3://{bucket_name}" in path:
            prefix = path.replace(f"s3://{bucket_name}",'')
            
        if not self.check_path_exist(path=path):
            self.logger.info("Folder path not existed: %s", path)
            return False
        
        return True
        
    
# from configs.envs.env_minio import ConfigMinIO
# S3MinIOObj = S3Ceph(aws_access_key_id=ConfigMinIO.aws_access_key_id,
#                     aws_secret_access_key=ConfigMinIO.aws_secret_access_key,
#                     endpoint_url=ConfigMinIO.endpoint_url)


if __name__ == '__main__':
    import sys
    import os
    config_dir = os.path.join('../', '')
    sys.path.insert(0, config_dir)

    from configs.envs.env_s3_ceph_config import ConfigS3Ceph

    s3_client = S3Ceph(aws_access_key_id=ConfigS3Ceph.aws_access_key_id,
                        aws_secret_access_key=ConfigS3Ceph.aws_secret_access_key,
                        endpoint_url=ConfigS3Ceph.endpoint_url)

    list_bucket = s3_client.get_list_bucket()
    print("List bucket: ", list_bucket)






    


