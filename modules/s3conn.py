import os
import boto3

class s3conn:
    
    def __init__(self):
        self.s3 = boto3.client('s3')
        
    def download(self, target_filepath, bucket, bucket_filepath):
        try:
            with open(target_filepath, 'wb') as data:
                self.s3.download_fileobj(bucket, bucket_filepath, data)
            return { "result": "success"}
        except Exception as err:
            return { "result": "error", "error_message": "s3 download failed - " +  str(err)}
            
    def list(self, bucket, prefix):
        objects = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return objects["Contents"]
        
    def upload(self, source_filepath, bucket, bucket_filepath, extra_args = {}):
        if os.path.isfile(source_filepath):
            try:
                with open(source_filepath, "rb") as f:
                    self.s3.upload_fileobj(f, bucket, bucket_filepath, ExtraArgs=extra_args )
                return { "result": "success"}
            except Exception as err:
                return { "result": "error", "error_message": "s3 upload failed - " +  str(err)}
        else:
            return { "result": "error", "error_message": "source file not found"}

