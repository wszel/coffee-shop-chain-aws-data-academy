import boto3
from botocore.exceptions import ClientError

def send_email():
    SENDER = 'YOUR EMAIL ADDRESS' 
    send_to = ['RECIPIENT EMAIL ADDRESS']
    # all addresses must be verified in Amazon SES
    
    AWS_REGION = 'eu-west-1'

    
    SUBJECT = "Final reports for friday 16.12"
    
    # the email text
    BODY_TEXT = ("Hello!"
    "We would like to present the results of our work during these intense three weeks. Please, see the attached results :)"
    "Access Your Bucket"
    "Kind regards,"
    "Group 1")
                
    # the email html body
    BODY_HTML = """<html>
    <head></head>
    <body>
    <h1>Hello!</h1>
    <p>We would like to present the results of our work during these intense three weeks. Please, see the attached results :)
    <br><a href='https://aws.amazon.com/s3/'>Access Your Bucket!</a>
    <br>Kind regards,
    <br>Group 1
    </p>
    </body>
    </html>
    """            

    # encoding for the email
    CHARSET = "UTF-8"

    client = boto3.client('ses',region_name=AWS_REGION)

    # send the email
    try:
    
        response = client.send_email(
            Destination={
                'ToAddresses': send_to,
            },
            Message={
                'Body': {
                    'Html': {
        
                        'Data': BODY_HTML
                    },
                    'Text': {
        
                        'Data': BODY_TEXT
                    },
                },
                'Subject': {

                    'Data': SUBJECT
                },
            },
            Source=SENDER
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def lambda_handler(event, context):
    send_email()es(bucket_name, file_names)