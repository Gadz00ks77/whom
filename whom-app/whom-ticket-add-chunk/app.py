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

        if not event['headers']:
            return {
                'statusCode': 400,
                'body': j.dumps({
                    'result':'failure',
                    'note':'no ticket guid in request headers'
                })
            }

        if 'ticketguid' not in event['headers']:
            return {
                'statusCode': 400,
                'body': j.dumps({
                    'result':'failure',
                    'note':'no ticket guid in request headers'
                })
            }
        elif len(event['headers']['ticketguid'])==0:
            return {
                'statusCode': 400,
                'body': j.dumps({
                    'result':'failure',
                    'note':'no ticket guid in request headers'
                })
            }
        else:
            identityticketuuid = event['headers']['ticketguid']


        # check if guid exists and is a valid 'BATCH' submit ticket

        if check_ticket_guid(identityticketuuid) == 1:
            return {
                'statusCode': 400,
                'body': j.dumps({
                    'result':'failure',
                    'note':'supplied ticket guid is either not available or is not a batch ticket',
                    'supplied guid':identityticketuuid
                })
            }

        content = event['body']
        content_object = j.loads(event['body'])

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
        put_to_s3(identityticketuuid,content,content_s3_key)
        add_chunk_key(identityticketuuid=identityticketuuid,ticket_chunk_s3_key=content_s3_key,objects=cnt)
        update_ticket_guid(ticket_guid=identityticketuuid,updatetimestr=dt_string)

        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS" # Allow only GET, POST request 
            },
            'body': j.dumps({
                'result':identityticketuuid,
                'comment':content_s3_key
            })
        }

    except Exception as e:
        b = bytes(str(e), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_ticket_chunk_add_error.log')    
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

def check_ticket_guid(ticket_guid):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_ticket',
        Key={
            'ticket_guid': {'S': ticket_guid}
        }
    )
    
    if 'Item' in response:
        if response['Item']['submit_method']['S']=='BATCH':
            return 0
        else: 
            return 1
    else: 
        return 1

def update_ticket_guid(ticket_guid,updatetimestr):

    table = boto3.resource('dynamodb').Table('whom_ticket')
    response = table.update_item(
        Key={
            'ticket_guid':ticket_guid
        },
        UpdateExpression="set ticket_updatedon = :u",
        ExpressionAttributeValues={
            ':u': updatetimestr,
        },
        ReturnValues="UPDATED_NEW"
    )

    return 0

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
                'last_updated_on': dt_string,
                'update_reason':        'NEW'
                }

    resp = table.put_item(Item=item)

