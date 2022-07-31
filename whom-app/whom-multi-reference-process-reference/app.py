from re import I
from xml.dom import ValidationErr
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

    b = bytes(str(event), 'utf-8')
    f = io.BytesIO(b)
    s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_multi_process_reference.log')    

    try:
        # BATCH SIZE IS ONE - SO THE INITIAL LOOP IS SIMPLY TO GET A SINGLE RECORD
        for record in event['Records']:
            reference_nk = record['dynamodb']['Keys']['reference_nk']['S']
            new_image = record['dynamodb']['NewImage']
            reference_object = new_image['reference_object']['M']
            set_nk = new_image['set_nk']['S']
            # ticket_chunk_s3key = set_object['ticket_chunk_s3key']
            system_reference = reference_object['system reference']['S']
            source = reference_object['source']['S']


            existing_identity = fetch_reference_and_source(system_reference=system_reference,source=source)

            # NEED A CHECK HERE FOR FINDING A MATCHING IDENTITY BUT FOR THE WRONG IDENTITY OBJECT TYPE

            if existing_identity != '':
                add_identity_to_set(set_nk,existing_identity)
            
            increment_set_checked_refs_count(set_nk)

            return {
                'statusCode': 200,
                'body': j.dumps({
                    'result':'success',
                })
            }

    except Exception as e:
        b = bytes(str(e)+'\n'+str(event), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_multi_process_reference_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def add_identity_to_set(set_nk,identity_guid):

    new_identity_chain = [
            {
                'found_identity':identity_guid
        }
        ]
    
    table = boto3.resource('dynamodb').Table('whom_ticket_stage_multi_set_idents')
    
    try:
        response = table.update_item(
            Key={
                'set_nk':set_nk
            },
            UpdateExpression="set identity_chain = list_append(identity_chain,:vals)",
            ExpressionAttributeValues={
                ':vals': new_identity_chain,
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        response = table.update_item(
            Key={
                'set_nk':set_nk
            },
            UpdateExpression="set identity_chain = :vals",
            ExpressionAttributeValues={
                ':vals': new_identity_chain,
            },
            ReturnValues="UPDATED_NEW"
        )
        

    return response

def fetch_reference_and_source(system_reference,source):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_references',
        Key={
            'system_reference': {'S': system_reference}
        }
    )    
    found = ''

    if 'Item' in response:
            for map in response['Item']['identity_map']['L']:
                if map['M']['source']['S']==source:
                    found = map['M']['identity_guid']['S']

    return found

def increment_set_checked_refs_count(set_nk):


    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    table = boto3.resource('dynamodb').Table('whom_ticket_stage_multi_reference_counter')
    response = table.update_item(
        Key={
            'set_nk': set_nk
        },
        UpdateExpression="set refs_in_set = refs_in_set, refs_in_set_completed = refs_in_set_completed + :c",
        ExpressionAttributeValues={
            ':c': 1
        },
        ReturnValues="UPDATED_NEW"
    )

    refset_count_received = response['Attributes']['refs_in_set']
    refset_count_completed = response['Attributes']['refs_in_set_completed']

    if refset_count_completed>=refset_count_received:
        update_set_status(set_nk)

def update_set_status(set_nk):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    table = boto3.resource('dynamodb').Table('whom_ticket_stage_multi_reference_set')
    response = table.update_item(
        Key={
            'set_nk': set_nk
        },
        UpdateExpression="set set_status = :s ",
        ExpressionAttributeValues={
            ':s': 'Reviewed'
        },
        ReturnValues="UPDATED_NEW"
    )

    return 0

# event = {'Records': [{'eventID': '3275c8cdc42538de0762959428ccbb5a', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'eu-west-1', 'dynamodb': {'ApproximateCreationDateTime': 1659083864.0, 'Keys': {'reference_nk': {'S': '969f6781-858b-4909-bb32-9c3f74ac757f'}}, 'NewImage': {'set_nk': {'S': '783958c0-5ea4-4843-9440-704e43b0280d'}, 'reference_nk': {'S': '969f6781-858b-4909-bb32-9c3f74ac757f'}, 'reference_object': {'M': {'system reference': {'S': '1002'}, 'source': {'S': 'ECLIPSE'}}}, 'ticket_chunk_s3key': {'S': '4b5e3416-8960-436e-a39e-2cb751eddb60/identity_object_efbdf560-c2d8-4cdb-9084-8637113f4dc2/783958c0-5ea4-4843-9440-704e43b0280d.json'}}, 'SequenceNumber': '23020300000000015528809424', 'SizeBytes': 341, 'StreamViewType': 'NEW_IMAGE'}, 'eventSourceARN': 'arn:aws:dynamodb:eu-west-1:266995720231:table/whom_ticket_stage_multi_references/stream/2022-07-24T10:57:54.453'}]}

# lambda_handler(event=event,context=None)