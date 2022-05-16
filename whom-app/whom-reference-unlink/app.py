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

        system_reference = event['pathParameters']['system-reference']
        source = event['pathParameters']['source']
        identity_object = event['pathParameters']['identity-object']
        identity_guid = event['pathParameters']['identity-guid']

        exists_outcome = chk_exists(system_reference,source=source,identity_object=identity_object)

        if exists_outcome['result']=='Empty':
            return {
                'statusCode':400,
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "GET, OPTIONS" # Allow only GET, POST request 
                },
                'body': j.dumps({
                    'result':event['pathParameters'],
                    'narrative':'provided system reference does not match any candidate.'
                })
            }

        if exists_outcome['specific item']['M']['identity_guid']['S']!=identity_guid:
            return {
                'statusCode':400,
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "GET, OPTIONS" # Allow only GET, POST request 
                },
                'body': j.dumps({
                    'result':event['pathParameters'],
                    'narrative':'provided identity guid is not matched to system reference.'
                })
            }

        output_obj = {
            'request':'UNLINK',
            'system_reference':system_reference,
            'new map':exists_outcome['result']['new_identity_map']
        }

        messageid = add_sqs_message(content=j.dumps(output_obj),system_reference=system_reference)

        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "GET, OPTIONS" # Allow only GET, POST request 
            },
            'body': j.dumps({
                'result':identity_guid
            })
        }

    except Exception as e:
        b = bytes(str(e), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_unlink_reference.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def chk_exists(reference,source,identity_object):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_references',
        Key={
            'system_reference': {'S': reference}
        }
    )    
    found = {}
    found['result'] = 'Empty'

    if 'Item' in response:
        for m in response['Item']['identity_map']['L']:
            if m['M']['source']['S']==source and m['M']['identity_object']['S']==identity_object:
                found['result'] = 'Yes'
                found['identity_map'] = response['Item']
                found['new_identity_map'] = new_map(identity_map_object=response['Item']['identity_map']['L'])
                found['specific item']=m

    return found


def new_map(identity_map_object,source,identity_object):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    for m in identity_map_object:
        if m['M']['source']['S']==source and m['M']['identity_object']['S']==identity_object:
            m['M']['delinked']=dt_string

    return identity_map_object

def add_sqs_message(content,system_reference):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomReferenceItems.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=system_reference,
        MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid