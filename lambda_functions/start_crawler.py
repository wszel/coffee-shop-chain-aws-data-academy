import json
import boto3
from botocore.exceptions import ClientError

def start_crawler(crawler_name):
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
      response = glue_client.start_crawler(Name=crawler_name)
      return response
    except ClientError as e:
      raise Exception("boto3 client error in start_a_crawler: " + e.__str__())
    except Exception as e:
      raise Exception("Unexpected error in start_a_crawler: " + e.__str__())

def lambda_handler(event, context):
    # get crawler name from event payload
    crawler_name = event['Input']
    print(crawler_name)
    start_crawler(crawler_name=crawler_name)