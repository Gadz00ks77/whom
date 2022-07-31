import boto3
import os
from datetime import datetime
import io
from boto3.dynamodb.conditions import Key
import json as j
import uuid
import hashlib

def lambda_handler(event,context):

    s3_errors = os.environ['S3ERRORS']
    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:
        # BATCH SIZE IS ONE - SO THE INITIAL LOOP IS SIMPLY TO GET A SINGLE RECORD
        for record in event['Records']:
            s3chunkguid = record['dynamodb']['Keys']['ticket_chunk_s3key']['S']
            s3chunkguidpathonly = s3chunkguid[0:s3chunkguid.find('.')]
            chunk_object = record['dynamodb']['NewImage']
            ticket_chunk_status = chunk_object['ticket_chunk_status']['S']
            # completed_cnt = chunk_object['completed_cnt']['N']
            # object_cnt = chunk_object['object_cnt']['N']
            update_reason = chunk_object['update_reason']['S']
            ticket_guid = chunk_object['ticket_guid']['S']

            ticket_metadata = get_ticket_metadata(ticketguid=ticket_guid)
            identity_object = ticket_metadata['identity_object_name']['S']
            submit_method = ticket_metadata['submit_method']['S']

            if ticket_chunk_status == 'PROCESSING' and update_reason == 'START' and submit_method in ['EVENT-SINGLE','BATCH-SINGLE']:

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
                    send_obj = {
                        'identity_object': identity_object,
                        'ticket_chunk_s3key': s3chunkguid,
                        'reference_object': reference_object
                    }
                    messageid = add_processor_message(content=j.dumps(send_obj),ticket_guid=ticket_guid,s3chunkguid=s3chunkguid)
                    update_ticket_status(ticket_guid=ticket_guid,to_status='PROCESSING')

            elif ticket_chunk_status == 'PROCESSING' and update_reason == 'START' and submit_method in ['EVENT-BYREFERENCE','BATCH-BYREFERENCE']:

                jreference_object = get_s3_object(s3chunkguid)
                reference_object = j.loads(jreference_object)

                if isinstance(reference_object,dict):
                    str_ref = str(reference_object)
                    # hash_object = hashlib.sha256(str_ref.encode('utf-8'))
                    hex_dig = str(uuid.uuid4()) # hash_object.hexdigest()
                    targets3key=s3chunkguidpathonly +'/'+hex_dig+'.json'
                    land_set_file(set_nk=hex_dig,content=jreference_object,target_multi_s3key=targets3key)
                    land_nk(hex_dig,s3chunkguid,targets3key)
                    land_count(hex_dig,1,s3chunkguid)
                    update_ticket_status(ticket_guid=ticket_guid,to_status='PROCESSING')
                elif isinstance(reference_object,list):
                    set_cnt = 1
                    for set in reference_object:
                        str_ref = str(set)
                        # hash_object = hashlib.sha256(str_ref.encode('utf-8'))
                        hex_dig = str(uuid.uuid4()) # hash_object.hexdigest()
                        targets3key=s3chunkguidpathonly +'/'+hex_dig+'.json'
                        land_set_file(set_nk=hex_dig,content=j.dumps(set),target_multi_s3key=targets3key)
                        land_nk(hex_dig,s3chunkguid,targets3key)
                        land_count(hex_dig,set_cnt,s3chunkguid)
                        

                    update_ticket_status(ticket_guid=ticket_guid,to_status='PROCESSING')

            else:
                pass

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

def add_sqs_message(content,system_reference,target_queue=None):

    if target_queue == None:
        TargetQueueName = 'WhomReferenceItems.fifo'
    else:
        TargetQueueName = target_queue

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=TargetQueueName)
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=system_reference,
        MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid

def add_processor_message(content,ticket_guid,s3chunkguid):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomChunkProcessor.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=ticket_guid,
        MessageDeduplicationId=s3chunkguid
    )
    messageid = response.get('MessageId')

    return messageid


def get_s3_object(s3key):

    s3_landing = os.environ['S3LANDING']

    s3 = boto3.resource('s3')
    obj = s3.Object(s3_landing, s3key)
    content = obj.get()['Body'].read().decode('utf-8') 

    return content

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

def land_nk(reference_object_hash,ticket_s3_key,set_s3_key):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_ticket_stage_multi_reference_set')


    set_item = {
                'set_nk':  reference_object_hash,
                'ticket_chunk_s3key': ticket_s3_key,
                'set_s3_key': set_s3_key,
                'set_status': 'Received'
                }

    resp = table.put_item(Item=set_item)

def land_count(reference_object_hash,set_cnt_received,ticket_s3_key):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_ticket_stage_multi_reference_counter')


    set_item = {
                'set_nk':  reference_object_hash,
                'ticket_chunk_s3key': ticket_s3_key,
                'set_count_received': set_cnt_received,
                'set_count_completed': 0,
                'refs_in_set_completed': 0
                }

    resp = table.put_item(Item=set_item)

def land_set_file(set_nk,content,target_multi_s3key):

    s3_landing = os.environ['S3MULTILANDING']
    s3_client = boto3.client('s3')

    b = bytes(str(content), 'utf-8')
    f = io.BytesIO(b)
    s3_client.upload_fileobj(f, s3_landing, target_multi_s3key) 
