import boto3
import os
from datetime import datetime
import io
from boto3.dynamodb.conditions import Key
import json as j

def lambda_handler(event,context):

    s3_errors = os.environ['S3ERRORS']
    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:
        # BATCH SIZE IS ONE - SO THE INITIAL LOOP IS SIMPLY TO GET A SINGLE RECORD
        for record in event['Records']:
            ticketguid = record['dynamodb']['Keys']['ticket_guid']['S']
            ticket_object = record['dynamodb']['NewImage']
            total_chunks = ticket_object['identity_object_chunks']['N']
            ticket_status = ticket_object['ticket_status']['S']
            identity_object_name = ticket_object['identity_object_name']['S']

        if ticket_status == 'RECEIVED':
            delivered_chunks_num = get_delivered_chunks(ticketguid=ticketguid) # TO:DO - Eventual consistency issues: need a companion Lambda that accounts for the fact that the delivered chunks may be lagged via an index not yet being updated.

            if int(total_chunks) == int(delivered_chunks_num):
                
                messageid = add_sqs_message(ticketguid,identityobjectname=identity_object_name,ticketguid=ticketguid)
                update_ticket_guid(ticket_guid=ticketguid,messageid=messageid,updatetimestr=dt_string)
                
                b  = bytes('success', 'utf-8')
                f = io.BytesIO(b)
                s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_send_to_sqs_send_success.log')    
                return {
                    'statusCode': 200,
                    'body': j.dumps({
                        'result':'success',
                        'note':'check s3 log'
                    })
                }           
            else:
                b  = bytes('insufficient chunks', 'utf-8')
                f = io.BytesIO(b)
                s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_send_to_sqs_chunks_success.log')    
                return {
                    'statusCode': 200,
                    'body': j.dumps({
                        'result':'success',
                        'note':'check s3 log'
                    })
                }  
        # else:
        #     b  = bytes(f'guid {ticketguid} was already sent to the queue', 'utf-8')
        #     f = io.BytesIO(b)
        #     s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_send_to_sqs_already_sent.log')    
        #     return {
        #         'statusCode': 200,
        #         'body': j.dumps({
        #             'result':'success',
        #             'note':'check s3 log'
        #         })
        #     }  

    except Exception as e:
        b = bytes(str(e)+'\n'+str(event), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_send_to_sqs_error.log')    
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

def get_delivered_chunks(ticketguid):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_ticket_chunk_keys')
    full_items = full_query(table,IndexName="ticket_guid-index",
                    KeyConditionExpression=Key('ticket_guid').eq(ticketguid))
    return len(full_items)

def add_sqs_message(content,identityobjectname,ticketguid):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomTickets.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=ticketguid,
        MessageDeduplicationId=ticketguid)
    messageid = response.get('MessageId')
    
    return messageid

def update_ticket_guid(ticket_guid,messageid,updatetimestr):

    table = boto3.resource('dynamodb').Table('whom_ticket')
    response = table.update_item(
        Key={
            'ticket_guid':ticket_guid
        },
        UpdateExpression="set ticket_updatedon = :u, ticket_status = :s, sqsmessageid = :m",
        ExpressionAttributeValues={
            ':u': updatetimestr,
            ':s': 'QUEUED',
            ':m': messageid
        },
        ReturnValues="UPDATED_NEW"
    )

    return 0