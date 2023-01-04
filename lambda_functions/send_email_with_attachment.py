import json
import smtplib
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
import urllib.parse
import boto3
from botocore.exceptions import ClientError
from datetime import date

def send_email(bucket_name, filenames):
    client = boto3.client(service_name = 'ses', region_name = 'eu-west-1')
    sent_subject = "Final reports for Friday 16.12"
    
    # creating a message
    msg = MIMEMultipart()
    msg['From'] = 'YOUR EMAIL ADDRESS'
    msg['To'] = ['RECIPIENT EMAIL ADDRESS'] # all addresses must be verified in Amazon SES
    msg['Subject'] = sent_subject

    message = """
    Hello! 
    We would like to present the results of our work during these intense three weeks. Please, see the attached results :)
    Kind regards,
    Group 1
    """

    msg.attach(MIMEText(message, 'plain'))  # adding the text to the msg as plain text

   # get files form s3 bucket
    for file in filenames:
        today = date.today()
        d1 = today.strftime("%d-%m-%Y")
        file = 'reports' + d1 + file
        s3 = boto3.client('s3')
        key = urllib.parse.unquote_plus(file, encoding='utf-8')
        localfile = '/tmp/' + file
        with open(localfile, 'wb') as f:
            s3.download_fileobj(bucket_name, key, f)
                
        # prepaing the attachment to be attached
        attachment = open(localfile, 'rb')  # opening in reading bite mode

        p = MIMEBase('application', 'octet-stream')
        p.set_payload(attachment.read())  # payload means content

        encoders.encode_base64(p)
        p.add_header('Content-Disposition', f'attachment; filename={file}')
        msg.attach(p)

        text = msg.as_string()  # converting the message to string so that it can be sent

    destination = { 'ToAddresses' : [msg['To']], 'CcAddresses' : [], 'BccAddresses' : []}
#  smtp.gmail.com
    try:
        result = client.send_raw_email(Source = msg['From'], Destinations = msg['To'], RawMessage = {'Data': text.as_string()})
        return {'message': 'error','status' : 'fail'} if 'ErrorResponse' in result else {'message': 'mail sent successfully', 'status' : 'success'}
    except ClientError as e:
        return {'message': e.response['Error']['Message'],'status' : 'fail'}


def lambda_handler(event, context):
    bucket = event['Bucket']
    filenames = event['Filename']
    
    send_email(bucket, filenames)