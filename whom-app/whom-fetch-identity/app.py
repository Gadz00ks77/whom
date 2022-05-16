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

        identity_guid = event['pathParameters']['identity-guid']

        identity_object = fetch_identity(identity_guid=identity_guid)

        if identity_object == {}:
            return {
                'statusCode':400,
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "GET, OPTIONS" # Allow only GET, POST request 
                }
            }    

        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "GET, OPTIONS" # Allow only GET, POST request 
            },
            'body': j.dumps({
                identity_object
            })
        }

    except Exception as e:
        b = bytes(str(e)+'\n'+(str(event)), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_fetch_identities.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def fetch_identity(identity_guid):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_identities',
        Key={
            'identity_guid': {'S': identity_guid}
        }
    )    
    found = {}

    if 'Item' in response:
        found = response['Item']

    return found
