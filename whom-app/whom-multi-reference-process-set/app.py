import boto3
import os
from datetime import datetime
import io
from boto3.dynamodb.conditions import Key
import json as j
import uuid
import hashlib
import copy

def lambda_handler(event,context):

    s3_errors = os.environ['S3ERRORS']
    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:
        # BATCH SIZE IS ONE - SO THE INITIAL LOOP IS SIMPLY TO GET A SINGLE RECORD
        for record in event['Records']:
            set_nk = record['dynamodb']['Keys']['set_nk']['S']
            set_object = record['dynamodb']['NewImage']
            set_status = set_object['set_status']['S']
            ticket_chunk_s3key = set_object['ticket_chunk_s3key']['S']
            set_s3key = set_object['set_s3_key']['S']
            
            if set_status == 'Received':
                get_and_split(s3key=set_s3key,set_nk=set_nk)
            elif set_status == 'Reviewed':

                identity_search_result = fetch_set_idents(set_nk=set_nk)

                if identity_search_result == {}:
                    update_me(set_nk=set_nk,new_status='Get First')

                elif identity_search_result['outcome'] == 'fail':
                    update_me(set_nk=set_nk,new_status='Fail Set')

                elif identity_search_result['outcome'] == 'success':
                    add_to_identity = identity_search_result['identity_guid']
                    update_me(set_nk=set_nk,new_status='Found Identity')

            elif set_status == 'Get First':
                pass
            elif set_status == 'Found Identity':
                pass
            elif set_status == 'Fail Set':
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
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_multi_process_set_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def get_s3_object(s3key):

    s3_landing = os.environ['S3MULTILANDING']

    s3 = boto3.resource('s3')
    obj = s3.Object(s3_landing, s3key)
    content = obj.get()['Body'].read().decode('utf-8') 

    return content

def get_and_split(s3key,set_nk):

    obj = get_s3_object(s3key=s3key)
    set_objects = j.loads(obj)

    ref_cnt = 0

    if isinstance(set_objects,dict):
        for refs in set_objects['reference set']:
            add_new_reference_nk(refs,set_nk,s3key)
            ref_cnt = ref_cnt + 1
        update_set_counter(set_nk=set_nk,set_counter=1,ref_counter=ref_cnt)
    elif isinstance(set_objects,list):
        cnt = 1
        for set_object in set_objects:
            cnt = cnt + 1
            for refs in set_object['reference set']:
                add_new_reference_nk(refs,set_nk,s3key)
                ref_cnt = ref_cnt + 1
        update_set_counter(set_nk,cnt,ref_cnt)
        
def update_set_counter(set_nk,set_counter,ref_counter):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    table = boto3.resource('dynamodb').Table('whom_ticket_stage_multi_reference_counter')
    response = table.update_item(
        Key={
            'set_nk': set_nk
        },
        UpdateExpression="set sets_processed = :c, refs_in_set = :r",
        ExpressionAttributeValues={
            ':c': set_counter,
            ':r': ref_counter
        },
        ReturnValues="UPDATED_NEW"
    )

    return 0

def update_me(set_nk,new_status):

    table = boto3.resource('dynamodb').Table('whom_ticket_stage_multi_reference_set')
    response = table.update_item(
        Key={
            'set_nk': set_nk
        },
        UpdateExpression="set set_status = :s",
        ExpressionAttributeValues={
            ':s': new_status
        },
        ReturnValues="UPDATED_NEW"
    )

    return 0

def add_new_reference_nk(reference_object,set_nk,ticket_s3_key):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_ticket_stage_multi_references')

    str_ref = str(reference_object)
    hex_dig = str(uuid.uuid4()) # hash_object.hexdigest()

    reference_item = {
                'reference_nk':  hex_dig,
                'set_nk': set_nk,
                'ticket_chunk_s3key': ticket_s3_key,
                'reference_object': reference_object
                }

    resp = table.put_item(Item=reference_item)

def fetch_set_idents(set_nk):

    table = boto3.resource('dynamodb').Table('whom_ticket_stage_multi_set_idents')
    response = table.get_item(
        Key={
            'set_nk': set_nk
        }
    )    

    result = {}

    if 'Item' in response:
        if 'identity_chain' in response['Item']:
            len_chain = unique_idents(response['Item']['identity_chain'])
            if len_chain > 1: 
                result = {'outcome':'fail','reason':'multiple competing identities for same object'}
                return result
            else:
                for f in response['Item']['identity_chain']:
                    result = {'outcome':'success','reason':'found identity','identity_guid':f['found_identity']}
                
                return result
    else:
        return result 

def unique_idents(chain):

    ident_list = []

    for c in chain:
        this_ident = c['found_identity']
        if this_ident not in ident_list:
            ident_list.append(ident_list)

    return len(ident_list)

def get_ref_set_and_send(set_nk,s3key,chunk_key,risk_object,existing_identity):

    obj = get_s3_object(s3key=s3key)
    set_objects = j.loads(obj)

    ref_cnt = 0

    out_obj = {
        'identity_object':risk_object,
        'ticket_chunk_s3_key': chunk_key,
        'reference_object': []
    }

    if isinstance(set_objects,dict):
        for refs in set_objects['reference set']:
            system_reference = refs['system reference']
            source = refs['source']
            ref_obj = {
                'system reference': system_reference,
                'source': source,
                'identity guid': existing_identity
            }
            out_obj['reference_object'].append(copy.deepcopy())
        
    elif isinstance(set_objects,list):
        cnt = 1
        for set_object in set_objects:
            cnt = cnt + 1
            for refs in set_object['reference set']:
                system_reference = refs['system reference']
                source = refs['source']
                ref_obj = {
                    'system reference': system_reference,
                    'source': source,
                    'identity guid': existing_identity
                }
                out_obj['reference_object'].append(copy.deepcopy())

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