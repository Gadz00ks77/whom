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
            reference_object = j.loads(record['body'])
            system_reference = reference_object['system reference']
            source = reference_object['source']
            identity_object_name = reference_object['identity_object']
            ticket_chunk_s3key = reference_object['ticket_chunk_s3key']

            if 'identity_guid' in reference_object:
                identity_guid = reference_object['identity_guid']
            else:
                identity_guid = ""

            exist_outcome = chk_exists(system_reference,source,identity_object_name)

            if len(identity_guid)>0:

                identity_object = get_identity(identity_guid=identity_guid)

                if identity_object == {}:
                    # THE IDENTITY YOU QUOTED DOESN'T EXIST
                    # EXIT HERE: THIS MESSAGE
                    message = {'outcome':'failed','object':reference_object,'reason':'quoted identity does not exist'}
                    add_sqs_message(j.dumps(message),ticket_chunk_s3key)

                if identity_object['identity_object_name']['S'] != identity_object_name:
                    # USER IS TRYING TO ADD A REFERENCE TO AN IDENTITY THAT IS NOT THIS IDENTITY OBJECT TYPE (e.g. A reference for a LAYER, to an identity for a RISK)
                    # EXIT HERE: PASS A MESSAGE IN THE TICKET THAT THEY'VE COCKED UP
                    message = {'outcome':'failed','object':reference_object,'reason':'quoted identity object type does not match'}
                    add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                else:
                    if exist_outcome['result']=='Empty':
                        add_new_reference(system_reference=system_reference,source=source,identity_object=identity_object_name,identity_guid=identity_guid)
                        modify_identity(reference=system_reference,source_ticket='tbc',identity_guid=identity_guid)
                        message = {'outcome':'success','object':reference_object,'reason':'new reference, added to identity'}
                        add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                    elif exist_outcome['result']=='Reference,Source,Object':
                        # CHECK IF PASSED IDENTITY MATCHES
                        if exist_outcome['identity_map']['identity_guid']['S'] == identity_guid:
                            message = {'outcome':'success','object':reference_object,'reason':'no action'}
                            add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                            # EXIT HERE: I've seen this before, and it's valid
                        else:
                            # EXIT HERE: I've seen this before, but you've given me the wrong identity_guid
                            message = {'outcome':'failed','object':reference_object,'reason':'reference object found but provided identity guid is invalid'}
                            add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                    elif exist_outcome['result'] in ['Reference,Object','Reference,Source','Reference Only']:
                        if exist_outcome['identity_map']['identity_guid']['S'] == identity_guid:
                            modify_reference(source=source,identity_object=identity_object_name,identity_guid=identity_guid)
                            modify_identity(reference=system_reference,source_ticket='tbc',identity_guid=identity_guid)
                            message = {'outcome':'success','object':reference_object,'reason':'modified reference and identity objects'}
                            add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                        else:
                            # EXIT HERE: I've seen this before in some capacity, but you've given me the wrong identity_guid
                            message = {'outcome':'failed','object':reference_object,'reason':'reference object valid, but identity guid is incorrect'}
                            add_sqs_message(j.dumps(message),ticket_chunk_s3key)

            else:
                # CHECK IF SUPPLIED REFERENCE HAS AN IDENTITY
                if exist_outcome['result']=='Reference,Source,Object':
                    # YES - SO USE THAT.
                    identity_guid = exist_outcome['identity_map']['identity_guid']['S']
                    # UPDATE REFERENCE OBJECT TO INCLUDE GUID AND 
                    reference_object['identity_guid'] = identity_guid
                    message = {'outcome':'success','object':reference_object,'reason':'identity found for system reference & context'}
                    add_sqs_message(j.dumps(message),ticket_chunk_s3key)                    
                    # EXIT HERE!
                else:
                    # NO - SO;
                    # 'CREATE IDENTITY'
                    identity_guid = get_new_identity(identity_object_name=identity_object_name,source_ticket='tbc',source=source,system_reference=system_reference)

                if exist_outcome['result']=='Empty':
                    add_new_reference(system_reference=system_reference,source=source,identity_object=identity_object_name,identity_guid=identity_guid)
                    reference_object['identity_guid'] = identity_guid
                    message = {'outcome':'success','object':reference_object,'reason':'new reference and identity generated'}
                    add_sqs_message(j.dumps(message),ticket_chunk_s3key)  
                else:
                    modify_reference(reference=system_reference,source=source,identity_object=identity_object_name,identity_guid=identity_guid)
                    reference_object['identity_guid'] = identity_guid
                    message = {'outcome':'success','object':reference_object,'reason':'new identity generated and added to reference object'}
                    add_sqs_message(j.dumps(message),ticket_chunk_s3key)  
                    
        b = bytes(str('success\n'+system_reference), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_ticket_process_chunk.log')    

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
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_ticket_process_chunk_error.log')    
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
        for m in response['Item']['Identity Map']['M']:
            if m['source']['S']==source and m['identity_object']['S']==identity_object:
                found['result'] = 'Reference,Source,Object'
                found['identity_map'] = m
            elif m['source']['S']!=source and m['identity_object']['S']==identity_object:
                found['result'] = 'Reference,Object'
            elif m['source']['S']==source and m['identity_object']['S']!=identity_object:
                found['result'] = 'Reference,Source'
            elif m['source']['S']!=source and m['identity_object']['S']!=identity_object:
                found['result'] = 'Reference Only'

    return found

def get_new_identity(identity_object_name,source_ticket,source,system_reference):

    new_identity = str(uuid.uuid4)

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identities')

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    item = {
        'identity_guid':new_identity,
        'identity_object_name':identity_object_name,
        'identity_chain':[
            {
                'source':source,
                'system_reference':system_reference,
                'source_ticket_guid':source_ticket
        }
        ],
        'identity_createdon':dt_string,
        'identity_updatedon':dt_string
    }

    resp = table.put_item(Item=item)

    return new_identity

def add_new_reference(system_reference,source,identity_object,identity_guid):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_references')

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    item = {
                'system_reference':     system_reference,
                'identity_map': [
                    {
                        'source':source,
                        'identity_object':identity_object,
                        'identity_guid': identity_guid
                    }
                ]
                }

    resp = table.put_item(Item=item)

def get_reference(reference):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_references',
        Key={
            'system_reference': {'S': reference}
        }
    )    

    if 'Item' in response:
        return response['Item']
    else: 
        return {}

def modify_reference(reference,source,identity_object,identity_guid):

    new_identity_map =  [{
        'identity_object':  identity_object,
        'source':           source,
        'identity_guid':    identity_guid
    }]

    table = boto3.resource('dynamodb').Table('whom_references')
    response = table.update_item(
        Key={
            'system_reference':reference
        },
        UpdateExpression="set identity_map = list_append(identity_map,:vals)",
        ExpressionAttributeValues={
            ':vals': new_identity_map,
        },
        ReturnValues="UPDATED_NEW"
    )

    return response

def get_identity(identity_guid):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_identities',
        Key={
            'identity_guid': {'S': identity_guid}
        }
    )    

    if 'Item' in response:
        return response['Item']
    else: 
        return {}

def modify_identity(reference,source,source_ticket,identity_guid):

    new_identity_chain = [
            {
                'source':source,
                'system_reference':reference,
                'source_ticket_guid':source_ticket
        }
        ]
    
    table = boto3.resource('dynamodb').Table('whom_identities')
    
    response = table.update_item(
        Key={
            'system_reference':reference
        },
        UpdateExpression="set identity_chain = list_append(identity_chain,:vals)",
        ExpressionAttributeValues={
            ':vals': new_identity_chain,
        },
        ReturnValues="UPDATED_NEW"
    )

    return response

def add_sqs_message(content,s3chunkkey):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomReturns.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=s3chunkkey,
        MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid