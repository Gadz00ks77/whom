from xml.dom.minidom import Identified
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
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_object_aggregates')
    
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:

        identityobjectaggregatename = event['pathParameters']['identity-object-aggregate-name']
        
        aggregate_item = {
                'identity_object_aggregate':   identityobjectaggregatename
                ,'aggregate_createdon':                dt_string
                ,'aggregaate_updatedon':                dt_string
                }

        resp = table.put_item(Item=aggregate_item)

        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "content-type": "application/json",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only POST request 
            },
            'body': j.dumps({
                'result':identityobjectaggregatename
            })
        }

    except Exception as e:
        b = bytes(str(e), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_aggregation_creation_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }
