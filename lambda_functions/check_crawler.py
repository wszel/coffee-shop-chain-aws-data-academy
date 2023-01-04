import boto3
import time
from botocore.exceptions import ClientError

def check_crawler(crawler_name):
    session = boto3.session.Session()
    glue_client = session.client('glue')
    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        status = response['Crawler']['State']
        print(status)
        if status == 'READY':
            print('Crawler finished the job')
            break
        else:
            print('waiting 15 seconds for crawler to finish')
        time.sleep(15)
def lambda_handler(event, context):
   # get crawler name from event payload
    crawler_name = event['Input']
    print(crawler_name)
    check_crawler(crawler_name=crawler_name)