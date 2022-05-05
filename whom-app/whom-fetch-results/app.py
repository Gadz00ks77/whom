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

        ticket_guid = event['pathParameters']['ticket-guid']
        ticket_object = fetch_ticket(ticket_guid=ticket_guid)

        if ticket_object == {}:
            return {
                'statusCode':400,
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS" # Allow only GET, POST request 
                },
                'body': j.dumps({
                    'result':'failure - invalid guid',
                    'next_chunk':'n/a',
                    'previous_chunk':'n/a'
                })
            }

        ticket_status = ticket_object['ticket_status']

        if ticket_status['S'] != 'COMPLETE':
            return {
                'statusCode':400,
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS" # Allow only GET, POST request 
                },
                'body': j.dumps({
                    'result':'failure - ticket not completed',
                    'next_chunk':'n/a',
                    'previous_chunk':'n/a'
                })
            }

        ticket_chunks = fetch_chunks(ticketguid=ticket_guid)
        ticket_chunks_sorted = sorted(ticket_chunks, key=lambda d: d['last_updated_on']) 
        fetch_specific_chunk = ''
        found_chunk = 0

        if event['headers']:
            if 'chunkkey' in event['headers']:
                fetch_specific_chunk = event['headers']['chunkkey']
                i = 0
                for chunk in ticket_chunks_sorted:
                    if fetch_specific_chunk == chunk['ticket_chunk_s3key']:
                        found_chunk = 1
                    i = i + 1

        if fetch_specific_chunk != '' and found_chunk == 0:
            return {
                'statusCode':400,
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS" # Allow only GET, POST request 
                },
                'body': j.dumps({
                    'result':'failure - specific chunk does not exist',
                    'next_chunk':'n/a',
                    'previous_chunk':'n/a'
                })
            }

        return_chunk = {}
        next_chunk = 'n/a'

        if fetch_specific_chunk != '' and found_chunk > 0:
            return_chunk = fetch_results(chunk_key=fetch_specific_chunk)
            if len(ticket_chunks_sorted) > (i + 1):
                next_chunk = ticket_chunks_sorted[i+1]
        elif fetch_specific_chunk == '':
            fetch_specific_chunk = ticket_chunks_sorted[0]['ticket_chunk_s3key']
            return_chunk = fetch_results(chunk_key=fetch_specific_chunk)
            if len(ticket_chunks_sorted)>1:
                next_chunk = ticket_chunks_sorted[1]['ticket_chunk_s3key']
  
        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS" # Allow only GET, POST request 
            },
            'body': j.dumps({
              'result': return_chunk,
              'next_chunk': next_chunk  
            })
        }

    except Exception as e:
        b = bytes(str(e)+'\n'+(str(event)), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_fetch_results.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def full_query(table, **kwargs):
    response = table.query(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

def fetch_chunks(ticketguid):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_ticket_chunk_keys')
    full_items = full_query(table,IndexName="ticket_guid-index",
                KeyConditionExpression=Key('ticket_guid').eq(ticketguid))

    return full_items

def fetch_ticket(ticket_guid):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_ticket',
        Key={
            'ticket_guid': {'S': ticket_guid}
        }
    )    
    found = {}

    if 'Item' in response:
        found = response['Item']

    return found

def fetch_results(chunk_key):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_request_outcomes')
    full_items = full_query(table,IndexName="ticket_chunk_s3_key-index",
                KeyConditionExpression=Key('ticket_chunk_s3_key').eq(chunk_key))

    return full_items

# event = {
#     'headers':'',
#     'pathParameters':{
#         'ticket-guid':'6e9e418c-4705-4719-b68c-09c668c52180'
#     }
# }

# lambda_handler(event,context=None)