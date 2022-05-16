import boto3
import os
from datetime import datetime
import io
from boto3.dynamodb.conditions import Key
import json as j
import uuid

def lambda_handler(event,context):

    s3_errors = os.environ['S3ERRORS']
    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:
        # BATCH SIZE IS ONE - SO THE INITIAL LOOP IS SIMPLY TO GET A SINGLE RECORD
        for record in event['Records']:
            s3chunkguid = record['dynamodb']['Keys']['ticket_chunk_s3key']['S']
            chunk_object = record['dynamodb']['NewImage']
            ticket_chunk_status = chunk_object['ticket_chunk_status']['S']
            update_reason = chunk_object['update_reason']['S']
            ticket_guid = chunk_object['ticket_guid']['S']

        ticket_metadata = get_ticket_metadata(ticketguid=ticket_guid)
        identity_object = ticket_metadata['identity_object_name']['S']

        if ticket_chunk_status == 'PROCESSING' and update_reason == 'START':

            jreference_object = get_s3_object(s3chunkguid)
            reference_object = j.loads(jreference_object)
            
            if isinstance(reference_object,dict):
                reference_object['identity_object'] = identity_object
                reference_object['ticket_chunk_s3key'] = s3chunkguid
                output_object = {
                    'request':'MATCH',
                    'reference_object':reference_object
                }
                #j.dumps(reference_object)
                messageid = add_sqs_message(content=j.dumps(output_object),system_reference=reference_object['system reference'])
                update_ticket_status(ticket_guid=ticket_guid,to_status='PROCESSING')
            else:
                for obj in reference_object:
                    obj['identity_object'] = identity_object
                    obj['ticket_chunk_s3key'] = s3chunkguid
                    output_obj = {
                        'request':'MATCH',
                        'reference_object':obj
                    }
                    # jobj = j.dumps(obj)
                    messageid = add_sqs_message(content=j.dumps(output_obj),system_reference=obj['system reference'])
                update_ticket_status(ticket_guid=ticket_guid,to_status='PROCESSING')

            b = bytes(str('success')+'\n'+str(event), 'utf-8')
            f = io.BytesIO(b)
            s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_send_to_reference_sqs.log')    
            return {
                'statusCode': 200,
                'body': j.dumps({
                    'result':'success',
                })
            }

        elif ticket_chunk_status == 'COMPLETE':
            if check_all_chunks_complete(ticket_guid) == 1:
                update_ticket_status(ticket_guid,'COMPLETE')
        
        else:
            pass

        # b = bytes(str('success')+'\n'+str(event), 'utf-8')
        # f = io.BytesIO(b)
        # s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_send_to_reference_sqs.log')    
        return {
            'statusCode': 200,
            'body': j.dumps({
                'result':'success',
            })
        }

    except Exception as e:
        b = bytes(str(e)+'\n'+str(event), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_send_to_reference_sqs_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def add_sqs_message(content,system_reference):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomReferenceItems.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=system_reference,
        MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid

def get_s3_object(s3key):

    s3_landing = os.environ['S3LANDING']

    s3 = boto3.resource('s3')
    obj = s3.Object(s3_landing, s3key)
    content = obj.get()['Body'].read().decode('utf-8') 

    return content

def full_query(table, **kwargs):
    response = table.query(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

def get_ticket_metadata(ticketguid):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_ticket',
        Key={
            'ticket_guid': {'S': ticketguid}
        }
    )
    
    response_object = {}

    if 'Item' in response:
        return response['Item']   
    else: 
        return response_object

def check_all_chunks_complete(ticketguid):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_ticket_chunk_keys')
    full_items = full_query(table,IndexName="ticket_guid-index",
                KeyConditionExpression=Key('ticket_guid').eq(ticketguid))

    chunks = len(full_items) # TO DO: Change this to a direct chunknum lookup from ticket
    complete = 0

    for item in full_items:
        if item['ticket_chunk_status']=='COMPLETE':
            complete = complete + 1

    if complete == chunks:
        return 1 # ALL CHUNKS ARE COMPLETE
    else:
        return 0

def update_ticket_status(ticket_guid,to_status):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    table = boto3.resource('dynamodb').Table('whom_ticket')
    response = table.update_item(
        Key={
            'ticket_guid':ticket_guid
        },
        UpdateExpression="set ticket_updatedon = :u, ticket_status = :s",
        ExpressionAttributeValues={
            ':u': dt_string,
            ':s': to_status
        },
        ReturnValues="UPDATED_NEW"
    )

    return 0