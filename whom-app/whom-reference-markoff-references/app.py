import chunk
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

        for record in event['Records']:
            message_body = j.loads(record['body'])
            messageid = record['messageId']
            actual_object = message_body['object']
            ticket_chunk_s3_key = actual_object['ticket_chunk_s3key']
            system_reference = actual_object['system reference']
            source = actual_object['source']
            identity_object_name = actual_object['identity_object']
            outcome_result = message_body['outcome']
            reason = message_body['reason']
            actual_identity_guid = actual_object['identity_guid']

            if 'identity guid' in actual_object:
                passed_identity_guid = actual_object['identity guid']
            else:
                passed_identity_guid = ''

            chunk_metadata = get_chunk_metadata(ticket_chunk_s3_key)

            ticket_guid = chunk_metadata['ticket_guid']

            if 'completed_cnt' in chunk_metadata:
                new_completed_cnt = chunk_metadata['completed_cnt']+1
            else:
                new_completed_cnt = 1

            if new_completed_cnt >= chunk_metadata['object_cnt']:
                new_status = 'COMPLETE'
            else:
                new_status = chunk_metadata['ticket_chunk_status']

            update_chunk_status(ticket_chunk_s3_key,new_count=new_completed_cnt,new_status=new_status)
            insert_outcome_record(messageId=messageid,ticketguid=ticket_guid,system_reference=system_reference,source=source,identity_object_name=identity_object_name,outcome_result=outcome_result,passed_identity_guid=passed_identity_guid,actual_identity_guid=actual_identity_guid,reason=reason,ticketchunkkey=ticket_chunk_s3_key)

        # b = bytes(str('success\n'+str(message_body)), 'utf-8')
        # f = io.BytesIO(b)
        # s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_reference_markoff.log')    

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
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_reference_markoff_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def update_chunk_status(s3key,new_count,new_status):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    table = boto3.resource('dynamodb').Table('whom_ticket_chunk_keys')
    response = table.update_item(
        Key={
            'ticket_chunk_s3key':s3key
        },
        UpdateExpression="set last_updated_on = :u,ticket_chunk_status = :s,completed_cnt = :c, update_reason = :ur",
        ExpressionAttributeValues={
            ':u': dt_string,
            ':s': new_status,
            ':c': new_count,
            ':ur': 'MARK OFF'
        },
        ReturnValues="UPDATED_NEW"
    )

def get_chunk_metadata(s3key):

    table = boto3.resource('dynamodb').Table('whom_ticket_chunk_keys')
    response = table.get_item(Key={
        'ticket_chunk_s3key':s3key
    })

    if 'Item' in response:
        return response['Item']
    else:
        return {}

def insert_outcome_record(messageId,system_reference,source,identity_object_name,outcome_result,passed_identity_guid,actual_identity_guid,reason,ticketchunkkey,ticketguid):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_request_outcomes')

    org_item = {
            'messageid': messageId,
            'ticket_chunk_s3_key': ticketchunkkey,
            'ticket_guid': ticketguid,
            'system_reference': system_reference,
            'source': source,
            'identity_object_name': identity_object_name,
            'result': outcome_result,
            'passed_identity_guid': passed_identity_guid,
            'actual_identity_guid': actual_identity_guid,
            'reason': reason
            }

    resp = table.put_item(Item=org_item)