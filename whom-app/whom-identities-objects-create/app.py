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

        body = j.loads(event['body'])

        if 'object_name' not in body:
            return {
                'statusCode': 400,
                'body': j.dumps({
                    'result':'failure',
                    'note':'object name not provided'
                })
            }
        else:
            object_name = body['object_name']

        if 'object_attributes' not in body:
            return {
                'statusCode': 400,
                'body': j.dumps({
                    'result':'failure',
                    'note':'object attributes not provided'
                })
            }
        else:
            object_attributes = body['object_attributes']

        if not isinstance(object_attributes,dict):
            return {
                'statusCode': 400,
                'body': j.dumps({
                    'result':'failure',
                    'note':'object attributes must be json object'
                })
            }

        add_object_type(object_name=object_name,object_attributes=object_attributes)

        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "GET, OPTIONS" # Allow only GET, POST request 
            }
        }

    except Exception as e:
        b = bytes(str(e)+'\n'+(str(event)), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_identity_objects_create_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def add_object_type(object_name,object_attributes):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_objects')

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    item = {
                'identity_object_name':     object_name,
                'identity_object_attributes': object_attributes,
                'addedon': dt_string,
                'updatedon': dt_string
                }

    resp = table.put_item(Item=item)