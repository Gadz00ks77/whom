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
    table = dynamodb.Table('whom_ticket')
    
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:

        identityobjectname = event['pathParameters']['identity-object-name']
        submitmethod = event['pathParameters']['method']
        identityticketuuid = str(uuid.uuid4())
        content = event['body']
        content_object = j.loads(event['body'])

        # need schema checker here

        valid_check = validate_content_schema(content_as_object=content_object)
        
        if valid_check['result'] == 1:
                return {
                    'statusCode': 400,
                    'body': j.dumps({
                        'result':'failure',
                        'note':'payload did not match required schema'
                    })
                }              
        else:
            cnt = valid_check['cnt']
        
        content_s3_key = f'{identityticketuuid}/identity_object_{str(uuid.uuid4())}.json'

        if submitmethod == 'EVENT':
            identity_object_chunks = 1
        else:
            if not event['headers']:
                return {
                    'statusCode': 400,
                    'body': j.dumps({
                        'result':'failure',
                        'note':'BATCH method specified but total chunks not specified in request Header'
                    })
                }  
            if 'totalchunks' in event['headers']:
                identity_object_chunks = event['headers']['totalchunks']
            else:
                return {
                    'statusCode': 400,
                    'body': j.dumps({
                        'result':'failure',
                        'note':'BATCH method specified but total chunks not specified in request Header'
                    })
                }                

        org_item = {
                'ticket_guid':                      identityticketuuid
                ,'ticket_status':                   'RECEIVED'
                ,'submit_method':                   submitmethod
                ,'identity_object_name':            identityobjectname
                ,'ticket_createdon':                dt_string
                ,'ticket_updatedon':                dt_string
                ,'identity_object_chunks':          identity_object_chunks
                }

        resp = table.put_item(Item=org_item)
        put_to_s3(identityticketuuid,content,content_s3_key)
        add_chunk_key(identityticketuuid=identityticketuuid,ticket_chunk_s3_key=content_s3_key,objects=cnt)

        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS" # Allow only GET, POST request 
            },
            'body': j.dumps({
                'result':identityticketuuid
            })
        }

    except Exception as e:
        b = bytes(str(e), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_ticket_creation_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def put_to_s3(ticket_uuid,content,targets3key):

    s3_landing = os.environ['S3LANDING']

    s3_client = boto3.client('s3')

    b = bytes(str(content), 'utf-8')
    f = io.BytesIO(b)
    s3_client.upload_fileobj(f, s3_landing, targets3key) 


def validate_content_schema(content_as_object):

    output = {}
    cnt = 0

    if isinstance(content_as_object,list):
        for vals in content_as_object:
            cnt = cnt + 1
            if 'system reference' not in vals:
                output['result']=1
                return output
            if 'source' not in vals:
                output['result']=1
                return output
    elif isinstance(content_as_object,dict):
        cnt = 1
        if 'system reference' not in content_as_object:
            output['result']=1
            return output
        if 'source' not in content_as_object:
            output['result']=1
            return output
    else:
        output['result']=1
        return output

    output['result']=0
    output['cnt']=cnt

    return output

def add_chunk_key(identityticketuuid,ticket_chunk_s3_key,objects):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_ticket_chunk_keys')

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    item = {
                'ticket_chunk_s3key':   ticket_chunk_s3_key,
                'ticket_guid':          identityticketuuid,
                'object_cnt':           objects,
                'ticket_chunk_status':  'RECEIVED',
                'last_updated_on': dt_string
                }

    resp = table.put_item(Item=item)