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

    message = {}

    try:

        for record in event['Records']:
            body_object = j.loads(record['body'])
            request_type = body_object['request']

            if request_type == 'MATCH':

                reference_object = body_object['reference_object']
                system_reference = reference_object['system reference']
                source = reference_object['source']
                identity_object_name = reference_object['identity_object']
                ticket_chunk_s3key = reference_object['ticket_chunk_s3key']

                if 'identity guid' in reference_object:
                    identity_guid = reference_object['identity guid']
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
                        reference_object['identity_guid'] = identity_guid
                        message = {'outcome':'failed','object':reference_object,'reason':'quoted identity object type does not match'}
                        add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                    else:
                        if exist_outcome['result']=='Empty':
                            add_new_reference(system_reference=system_reference,source=source,identity_object=identity_object_name,identity_guid=identity_guid)
                            # SEND MESSAGE TO MODIFY IDENTITY

                            reference_object['identity_guid'] = identity_guid
                            message = {'outcome':'success','object':reference_object,'reason':'new reference, added to modify identity queue'}
                            add_modify_identity_message(j.dumps(message),identity_guid)

                            # OLD
                            # modify_identity(reference=system_reference,source_ticket='tbc',source=source,identity_guid=identity_guid)
                            # reference_object['identity_guid'] = identity_guid
                            # message = {'outcome':'success','object':reference_object,'reason':'new reference, added to identity'}
                            # add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                        elif exist_outcome['result']=='Reference,Source,Object':
                            # CHECK IF PASSED IDENTITY MATCHES
                            if exist_outcome['identity_map']['M']['identity_guid']['S'] == identity_guid:
                                reference_object['identity_guid'] = identity_guid
                                message = {'outcome':'success','object':reference_object,'reason':'no action'}
                                add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                                # EXIT HERE: I've seen this before, and it's valid
                            else:
                                # EXIT HERE: I've seen this before, but you've given me the wrong identity_guid
                                reference_object['identity_guid'] = exist_outcome['identity_map']['M']['identity_guid']['S'] 
                                message = {'outcome':'failed','object':reference_object,'reason':'reference object found but provided identity guid is invalid'}
                                add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                        elif exist_outcome['result'] in ['Reference,Object','Reference,Source','Reference Only']:
                            if exist_outcome['identity_map']['M']['identity_guid']['S'] == identity_guid:
                                modify_reference(source=source,identity_object=identity_object_name,identity_guid=identity_guid)
                                # SEND MESSAGE TO MODIFY IDENTITY

                                reference_object['identity_guid'] = exist_outcome['identity_map']['M']['identity_guid']['S'] 
                                message = {'outcome':'success','object':reference_object,'reason':'new reference, added to modify identity queue'}
                                add_modify_identity_message(j.dumps(message),identity_guid)

                                # OLD
                                # modify_identity(reference=system_reference,source_ticket='tbc',source=source,identity_guid=identity_guid)
                                # reference_object['identity_guid'] = exist_outcome['identity_map']['M']['identity_guid']['S'] 
                                # message = {'outcome':'success','object':reference_object,'reason':'modified reference and identity objects'}
                                # add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                            else:
                                # EXIT HERE: I've seen this before in some capacity, but you've given me the wrong identity_guid
                                reference_object['identity_guid'] = exist_outcome['identity_map']['M']['identity_guid']['S'] 
                                message = {'outcome':'failed','object':reference_object,'reason':'reference object valid, but identity guid is incorrect'}
                                add_sqs_message(j.dumps(message),ticket_chunk_s3key)

                else:
                    # CHECK IF SUPPLIED REFERENCE HAS AN IDENTITY
                    if exist_outcome['result']=='Empty':
                        identity_guid = get_new_identity(identity_object_name=identity_object_name,source_ticket='tbc',source=source,system_reference=system_reference)
                        add_new_reference(system_reference=system_reference,source=source,identity_object=identity_object_name,identity_guid=identity_guid)
                        reference_object['identity_guid'] = identity_guid
                        message = {'outcome':'success','object':reference_object,'reason':'new reference and identity generated'}
                        add_sqs_message(j.dumps(message),ticket_chunk_s3key)  
                    elif exist_outcome['result']=='Reference,Source,Object':
                        # YES - SO USE THAT.
                        identity_guid = exist_outcome['identity_map']['M']['identity_guid']['S']
                        # UPDATE REFERENCE OBJECT TO INCLUDE GUID AND # EXIT HERE!
                        reference_object['identity_guid'] = identity_guid
                        message = {'outcome':'success','object':reference_object,'reason':'identity found for system reference & context'}
                        add_sqs_message(j.dumps(message),ticket_chunk_s3key)                    
    
                    else:
                        # ANY OTHER REFERENCE RESPONSE;
                        # 'CREATE IDENTITY'
                        identity_guid = get_new_identity(identity_object_name=identity_object_name,source_ticket='tbc',source=source,system_reference=system_reference)
                        modify_reference(reference=system_reference,source=source,identity_object=identity_object_name,identity_guid=identity_guid)
                        reference_object['identity_guid'] = identity_guid
                        message = {'outcome':'success','object':reference_object,'reason':'new identity generated and added to reference object'}
                        add_sqs_message(j.dumps(message),ticket_chunk_s3key)  

            elif request_type=='UNLINK':
                
                new_map = body_object['new map']
                system_reference = body_object['system_reference']
                replace_identity_map(system_reference=system_reference,new_map=new_map)
                message = {'outcome':'success','object':new_map,'reason':'unlinked identity'}

            else:
                message = {}

                    
        # outfile(system_reference=j.dumps(message)+'\n\n'+str(event),s3_errors=s3_errors,s3_client=s3_client,timestring=dt_string)
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
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_reference_generate_error.log')    
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
                found['result'] = 'Reference,Source,Object'
                found['identity_map'] = m
            elif m['M']['source']['S']!=source and m['M']['identity_object']['S']==identity_object:
                found['result'] = 'Reference,Object'
            elif m['M']['source']['S']==source and m['M']['identity_object']['S']!=identity_object:
                found['result'] = 'Reference,Source'
            elif m['M']['source']['S']!=source and m['M']['identity_object']['S']!=identity_object:
                found['result'] = 'Reference Only'

    return found

def get_new_identity(identity_object_name,source_ticket,source,system_reference):

    new_identity = str(uuid.uuid4())

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
                        'source':           source,
                        'identity_object':  identity_object,
                        'identity_guid':    identity_guid
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
            'identity_guid':identity_guid
        },
        UpdateExpression="set identity_chain = list_append(identity_chain,:vals)",
        ExpressionAttributeValues={
            ':vals': new_identity_chain,
        },
        ReturnValues="UPDATED_NEW"
    )

    return response

def add_modify_identity_message(content,identity_guid):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomIdentitiesItems.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=identity_guid,
        MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid

def add_sqs_message(content,s3chunkkey):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomReturns')
    response = queue.send_message(
        MessageBody=content)
        # MessageBody=content,
        # MessageGroupId=s3chunkkey,
        # MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid

def outfile(system_reference,s3_errors,s3_client,timestring):
    
    b = bytes(str('success\n'+system_reference), 'utf-8')
    f = io.BytesIO(b)
    s3_client.upload_fileobj(f, s3_errors, f'whom_{timestring}_reference_generate.log')

    return 0    

def replace_identity_map(system_reference,new_map):

    table = boto3.resource('dynamodb').Table('whom_references')
    response = table.update_item(
        Key={
            'system_reference': system_reference
        },
        UpdateExpression="set identity_map = :m",
        ExpressionAttributeValues={
            ':m': new_map
        },
        ReturnValues="UPDATED_NEW"
    )

    return 0


event = {'Records': [{'messageId': '653d438e-4211-405c-8f1c-900629a474da', 'receiptHandle': 'AQEB4heGnKnF923QkISDzk879XS5uwZALE3REQF1nSEBSjsn5kjC7N2wTVj7OQv8jwPYiliKlBR+g7S69esyITbXafBpRN8rdcLRI+1aO6fjC+zkgploJv+OxvrqYglr4eZGdXTCKMtGkzm+rEVT+GjV4kCt7XK0RJU8EVTYJf4kIU2l02mnxPGPPx+28KQFMsT4e5BGDWvrzk/tXH0HHyPVvVh9yTflrexFnOpYyJQODRfpMtkGrU7f67hnIWE8AfjSWf+bTErp1Yg1Hq1cXwyecTwe1iwEb9U6Zwtw23zjhN8=', 'body': '{"request": "MATCH", "reference_object": {"system reference": "7006", "source": "ECLIPSE", "identity_object": "RISK", "ticket_chunk_s3key": "254eb055-5ab4-45bd-84fb-9fcd5564b7bc/identity_object_acdd4fee-addd-4594-880b-079a336828f1.json"}}', 'attributes': {'ApproximateReceiveCount': '1', 'SentTimestamp': '1657132367991', 'SequenceNumber': '18870969959915247872', 'MessageGroupId': '7006', 'SenderId': 'AROAT4KRPMAT5X6Z5QVM7:whom-app-whomTicketProcessChunk-nkz5C9N4KI2H', 'MessageDeduplicationId': '307f434f-4d72-4688-bfdb-b54cdc4bd158', 'ApproximateFirstReceiveTimestamp': '1657132367991'}, 'messageAttributes': {}, 'md5OfBody': 'cded9f0446475d01b3bdf9555e14154c', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:eu-west-1:266995720231:WhomReferenceItems.fifo', 'awsRegion': 'eu-west-1'}]}

lambda_handler(event=event,context=None)