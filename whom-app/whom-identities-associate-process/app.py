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

    message = {}

    try:

        for record in event['Records']:
            
            body_object = j.loads(record['body'])

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
        b = bytes(str(e)+'\n'+str(event), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_modify_identity_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def modify_identity(reference,source,source_ticket,identity_guid):

    new_identity_chain = [
            {
                'source':source,
                'system_reference':reference,
                'source_ticket_guid':source_ticket
        }
        ]
    
    table = boto3.resource('dynamodb').Table('whom_identities')
    
    response = table.update_item(
        Key={
            'identity_guid':identity_guid
        },
        UpdateExpression="set identity_chain = list_append(identity_chain,:vals)",
        ExpressionAttributeValues={
            ':vals': new_identity_chain,
        },
        ReturnValues="UPDATED_NEW"
    )

    return response



