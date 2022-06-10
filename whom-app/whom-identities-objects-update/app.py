import boto3
import os
from datetime import datetime
import io
import uuid
from boto3.dynamodb.conditions import Key
import json as j
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

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

        current_object = fetch_identity_object_type(object_name=object_name)
        current_version = current_object['version_uuid']
        archive_current_version(object_name=object_name,version_uuid=current_version,object_contents=current_object)
        version = update_object_type(object_name=object_name,object_attributes=object_attributes)

        return {
            'statusCode':200,
            'body':j.dumps({
                'result':'added object type',
                'object_version_uuid': version
            }),
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


def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}

def fetch_identity_object_type(object_name):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_identity_objects',
        Key={
            'identity_object_name': {'S': object_name}
        }
    )    
    found = {}

    if 'Item' in response:
        found = from_dynamodb_to_json(response['Item'])

    return found

def update_object_type(object_name,object_attributes):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    table = boto3.resource('dynamodb').Table('whom_identity_objects')
    version = str(uuid.uuid4())
    response = table.update_item(
        Key={
            'identity_object_name':object_name
        },
        UpdateExpression="set updatedon=:u, identity_object_attributes=:a, version_uuid=:v",
        ExpressionAttributeValues={
            ':u': dt_string,
            ':a': object_attributes,
            ':v': version
        },
        ReturnValues="UPDATED_NEW"
    )

    return version

def archive_current_version(object_name,version_uuid,object_contents):
    
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]
    s3_errors = os.environ['S3ATTRIBARCHIVE']
    s3_client = boto3.client('s3')    
    b = bytes(j.dumps(object_contents), 'utf-8')
    f = io.BytesIO(b)
    s3_client.upload_fileobj(f, s3_errors, f'{object_name}/{dt_string}/{version_uuid}.json')    