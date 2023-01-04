import boto3
from botocore.exceptions import ClientError
import time

def check_files(bucket_name, files):
    bucket = boto3.resource('s3').Bucket(bucket_name)
    
    for file in files:
        while True:
            key = file
            objects = list(bucket.objects.filter(Prefix=key))
            if any([obj.key == file for obj in objects]):
                print("Exists", file) # file exists
                break
            else: 
                print("Doesnt exist", file)
                time.sleep(30) # wait 30 seconds
                pass
        
def lambda_handler(event, context):
    bucket_name = event['Bucket']
    file_names = event['Files']
    check_files(bucket_name, file_names)