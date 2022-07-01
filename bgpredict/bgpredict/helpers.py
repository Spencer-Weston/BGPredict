import boto3


class S3Connection:

    def __init__(self):
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = 'bgpredict'
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.s3_client = boto3.client("s3")