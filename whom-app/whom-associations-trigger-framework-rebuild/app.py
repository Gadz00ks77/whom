import boto3
import os
from datetime import datetime
import io
import uuid
from boto3.dynamodb.conditions import Key
import json as j
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer
import copy
import ast


def lambda_handler(event,context):

    s3_errors = os.environ['S3ERRORS']
    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:
        # BATCH SIZE IS ONE - SO THE INITIAL LOOP IS SIMPLY TO GET A SINGLE RECORD
        for record in event['Records']:
            identityguid = record['dynamodb']['Keys']['identity_guid']['S']
            identity_object_type = fetch_identity_object_type(identity_guid=identityguid)
            association_object = record['dynamodb']['NewImage']
            set = association_object['association_set']['L']

            remove_populated_target(identity_guid=identityguid,identity_object_type=identity_object_type)

            for s in set:
                if s['M']['parent']['S']=='Parent':
                    for k in loop_to_root_parent(starting_guid=s['M']['to_identity_guid']['S']):
                        # trigger framework rebuild for stated root object via queue
                        content = j.dumps({'identity_guid':k})
                        messageid = add_sqs_message(content=content,at_time=dt_string,identityguid=k)

            return {
                'statusCode': 200,
                'body': j.dumps({
                    'result':'success'
                })
            }  

    except Exception as e:
        b = bytes(str(e)+'\n'+str(event), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_trigger_framework_rebuild_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def fetch_identity_association(identity_guid):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_identity_associations',
        Key={
            'identity_guid': {'S': identity_guid}
        }
    )    
    found = {}

    if 'Item' in response:
        found = response['Item']

    return found

def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}

class FullAssocSet:

    def __init__(self):
        self.data = {}

def fetch_all_associations(first_guid):

    FinalAssocSet = FullAssocSet()

    associations = {}

    current_guid_associations = fetch_identity_association(identity_guid=first_guid)
    pycurrent_guid_associations = from_dynamodb_to_json(current_guid_associations)

    FinalAssocSet.data[first_guid]=pycurrent_guid_associations['association_set']

    return FinalAssocSet.data

def recurse_identities(identity_guid,parent_guid):

    # yield identity_guid

    new_assoc = fetch_identity_association(identity_guid=identity_guid)
    clean_assoc = from_dynamodb_to_json(new_assoc)

    for assoc in clean_assoc['association_set']:
        new_guid = assoc['to_identity_guid']
        if parent_guid != new_guid:
            assoc['parent_guid']= identity_guid
            yield assoc
            for r in recurse_identities(new_guid,parent_guid=identity_guid):
                yield(r)


def loop_to_root_parent(starting_guid):
    
    assocs = fetch_all_associations(first_guid=starting_guid)

    has_parent = 0

    for a in assocs:
        for l in assocs[a]:
            if l['parent']=='Child':
                has_parent = 1
                for k in loop_to_root_parent(starting_guid=l['to_identity_guid']):
                    yield k
    
    if has_parent==0:
        yield starting_guid

def add_sqs_message(content,at_time,identityguid):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomFrameworkRebuild.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=identityguid,
        MessageDeduplicationId=identityguid+str(at_time))
    messageid = response.get('MessageId')
    
    return messageid


def fetch_identity_object_type(identity_guid):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_identities',
        Key={
            'identity_guid': {'S': identity_guid}
        }
    )    
    found = {}

    if 'Item' in response:
        object_type = response['Item']['identity_object_name']['S']

    return object_type

def remove_populated_target(identity_guid,identity_object_type):

    bucket = os.environ['POPUTARGE']
    s3 = boto3.resource('s3')

    s3key = f'{identity_object_type}/{identity_guid}/{identity_object_type}.json'

    obj_exists = list(s3.Bucket(bucket).objects.filter(Prefix=s3key))
    
    if len(obj_exists) > 0 and obj_exists[0].key == s3key:
        s3.Object(bucket,s3key).delete() 

    return 1
