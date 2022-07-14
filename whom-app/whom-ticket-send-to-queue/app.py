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
            completed_chunks = ticket_object['completed_chunks']['N']
            delivered_chunks_num = ticket_object['delivered_chunks']['N']

        if ticket_status == 'RECEIVED' and int(total_chunks) == int(delivered_chunks_num):
            
            messageid = add_sqs_message(ticketguid,identityobjectname=identity_object_name,ticketguid=ticketguid)
            update_ticket_guid(ticket_guid=ticketguid,to_status='QUEUED',updatetimestr=dt_string)
            
        elif completed_chunks >= total_chunks and ticket_status != 'COMPLETE': 
            update_ticket_guid(ticket_guid=ticketguid,to_status='COMPLETE',updatetimestr=dt_string)

        else:
            pass

        return {
            'statusCode': 200,
            'body': j.dumps({
                'result':'success',
                'note':'check s3 log'
            })
        }           

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

def add_sqs_message(content,identityobjectname,ticketguid):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomTickets.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=ticketguid,
        MessageDeduplicationId=ticketguid)
    messageid = response.get('MessageId')
    
    return messageid

def update_ticket_guid(ticket_guid,updatetimestr,to_status):

    table = boto3.resource('dynamodb').Table('whom_ticket')
    response = table.update_item(
        Key={
            'ticket_guid':ticket_guid
        },
        UpdateExpression="set ticket_updatedon = :u, ticket_status = :s",
        ExpressionAttributeValues={
            ':u': updatetimestr,
            ':s': to_status
        },
        ReturnValues="UPDATED_NEW"
    )

    return 0

