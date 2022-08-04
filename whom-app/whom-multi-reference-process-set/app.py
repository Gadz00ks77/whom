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
            set_identity_object_type = set_object['set_identity_object_type']['S']
            ticket_chunk_s3key = set_object['ticket_chunk_s3key']['S']
            set_s3key = set_object['set_s3_key']['S']

            # b = bytes(str(event), 'utf-8')
            # f = io.BytesIO(b)
            # s3_client.upload_fileobj(f, s3_errors, f'whom_{set_nk}_{set_status}_{dt_string}_multi_process_set_event.log')   
            
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
                get_ref_set_and_send_first(set_nk=set_nk,s3key=set_s3key,chunk_key=ticket_chunk_s3key,risk_object=set_identity_object_type)
                # b = bytes(str(event), 'utf-8')
                # f = io.BytesIO(b)
                # s3_client.upload_fileobj(f, s3_errors, f'whom_{set_nk}_GotFirst_{set_status}_{dt_string}_multi_process_set_event.log')   

            elif set_status in ['Found Identity','Get Rest']:
                identity_search_result = fetch_set_idents(set_nk=set_nk)
                add_to_identity = identity_search_result['identity_guid']
                get_ref_set_and_send(set_nk=set_nk,s3key=set_s3key,chunk_key=ticket_chunk_s3key,risk_object=set_identity_object_type,existing_identity=add_to_identity)
                # b = bytes(str(event), 'utf-8')
                # f = io.BytesIO(b)
                # s3_client.upload_fileobj(f, s3_errors, f'whom_{set_nk}_GotRest_{set_status}_{dt_string}_multi_process_set_event.log')   
            elif set_status == 'Fail Set':
                split_and_fail(s3key=set_s3key,set_nk=set_nk,object_type=set_identity_object_type,ticketchunks3key=ticket_chunk_s3key)
                
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

    for refs in set_objects['reference set']:
        add_new_reference_nk(refs,set_nk,s3key)
        ref_cnt = ref_cnt + 1
    update_set_counter(set_nk=set_nk,set_counter=1,ref_counter=ref_cnt)

def split_and_fail(set_nk,s3key,object_type,ticketchunks3key):

    obj = get_s3_object(s3key=s3key)
    set_objects = j.loads(obj)

    identity_search_result = fetch_set_idents(set_nk=set_nk)

    matched_idents = identity_search_result['idents']
    ident_list = []
    for m in matched_idents:
        ident_list.append(m['found_identity'])
    str_list = ','.join(ident_list)

    ref_cnt = 0

    for refs in set_objects['reference set']:
        system_reference = refs['system reference']
        source = refs['source']
        
        message = {
            'object':{
                'ticket_chunk_s3key':ticketchunks3key,
                'system reference': system_reference,
                'source':source,
                'identity_object':object_type,
                'identity_guid': str_list
            },
                'outcome':'fail - multiple matching identities',
                'reason':'one or more references had competing identities for the same object type'
        }

        add_sqs_message(content=j.dumps(message))
        
def add_sqs_message(content):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomReturns')
    response = queue.send_message(
        MessageBody=content)
        # MessageBody=content,
        # MessageGroupId=s3chunkkey,
        # MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid
        
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
                result = {'outcome':'fail','reason':'multiple competing identities for same object','idents':response['Item']['identity_chain']}
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
            ident_list.append(this_ident)

    return len(ident_list)

def add_processor_message(content,set_nk):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomChunkProcessor.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=set_nk,
        MessageDeduplicationId=str(uuid.uuid4())
    )
    messageid = response.get('MessageId')

    return messageid

def get_ref_set_and_send_first(set_nk,s3key,chunk_key,risk_object):

    s3_errors = os.environ['S3ERRORS']
    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    obj = get_s3_object(s3key=s3key)
    set_objects = j.loads(obj)
    ticket_guid = chunk_key[0:chunk_key.find('/')]

    set_type = 'FIRST'
    
    out_obj = {
        'identity_object':risk_object,
        'ticket_chunk_s3key': chunk_key,
        'set_type': set_type,
        'set_nk': set_nk,
        'reference_object': []
    }

    refs = set_objects['reference set'].pop(0)
    system_reference = refs['system reference']
    source = refs['source']

    ref_obj = {
        'system reference': system_reference,
        'source': source
    }

    out_obj['reference_object'].append(copy.deepcopy(ref_obj))
    
    messageid = add_processor_message(content=j.dumps(out_obj),set_nk=set_nk)

    # b = bytes(str(messageid), 'utf-8')
    # f = io.BytesIO(b)
    # s3_client.upload_fileobj(f, s3_errors, f'whom_{set_nk}_sentmessage_noexist_{dt_string}_multi_process_set_event.log')  

    return 0

def get_ref_set_and_send(set_nk,s3key,chunk_key,risk_object,existing_identity):

    s3_errors = os.environ['S3ERRORS']
    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    obj = get_s3_object(s3key=s3key)
    set_objects = j.loads(obj)
    ticket_guid = chunk_key[0:chunk_key.find('/')]

    ref_cnt = 0
    set_type = 'ADD'

    out_obj = {
        'identity_object':risk_object,
        'ticket_chunk_s3key': chunk_key,
        'set_type': set_type,
        'set_nk': set_nk,
        'reference_object': []
    }

    for refs in set_objects['reference set']:
        system_reference = refs['system reference']
        source = refs['source']

        ref_obj = {
            'system reference': system_reference,
            'source': source,
            'identity guid': existing_identity
        }
        out_obj['reference_object'].append(copy.deepcopy(ref_obj))

    messageid = add_processor_message(content=j.dumps(out_obj),set_nk=set_nk)

    # b = bytes(str(messageid), 'utf-8')
    # f = io.BytesIO(b)
    # s3_client.upload_fileobj(f, s3_errors, f'whom_{set_nk}_sentmessage_hasexist_{dt_string}_multi_process_set_event.log')  

    return 0

