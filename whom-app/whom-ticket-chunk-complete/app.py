import boto3
import os
from datetime import datetime
import io
import uuid
from boto3.dynamodb.conditions import Key
import json as j
from botocore.exceptions import ClientError

def lambda_handler(event,context):

    s3_errors = os.environ['S3ERRORS']

    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:

        for record in event['Records']:
            ticket_chunk_s3_key = record['body']
            update_chunk_status(ticket_chunk_s3_key)

        # b = bytes(str('success\n'+str(chunk_keys)), 'utf-8')
        # f = io.BytesIO(b)
        # s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_ticket_start_chunk_process.log')    

        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS" # Allow only GET, POST request 
            },
            'body': j.dumps({
                'result':'success'
            })
        }

    except Exception as e:
        b = bytes(str(e), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_ticket_complete_chunk_process_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def update_chunk_status(s3key):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    table = boto3.resource('dynamodb').Table('whom_ticket_chunk_keys')
    response = table.update_item(
        Key={
            'ticket_chunk_s3key':s3key
        },
        UpdateExpression="set last_updated_on = :u,ticket_chunk_status = :s, update_reason = :ur",
        ExpressionAttributeValues={
            ':u': dt_string,
            ':s': 'COMPLETE',
            ':ur': 'COMPLETED'
        },
        ReturnValues="UPDATED_NEW"
    )


