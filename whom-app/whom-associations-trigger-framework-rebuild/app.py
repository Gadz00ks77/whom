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
            association_object = record['dynamodb']['NewImage']
            set = association_object['association_set']['L']
            
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
            
# event = {'Records': [{'eventID': '1d1076a6400065c510ec6866d71fd7f6', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'eu-west-1', 'dynamodb': {'ApproximateCreationDateTime': 1654894705.0, 'Keys': {'identity_guid': {'S': 'ee53869c-f98d-4b29-b053-c7e01ddec669'}}, 'NewImage': {'association_set': {'L': [{'M': {'parent': {'S': 'Child'}, 'association_type': {'S': 'HAS'}, 'to_identity_guid': {'S': '6f4a2513-7a44-4ffe-a50b-66b5e8ef6f8c'}, 'cardinality': {'S': '?:1'}}}, {'M': {'parent': {'S': 'Parent'}, 'association_type': {'S': 'HAS'}, 'to_identity_guid': {'S': '0aba3396-8150-4657-b383-439beda0cfbc'}, 'cardinality': {'S': '1:?'}}}]}, 'identity_guid': {'S': 'ee53869c-f98d-4b29-b053-c7e01ddec669'}}, 'SequenceNumber': '108168800000000004222274514', 'SizeBytes': 325, 'StreamViewType': 'NEW_IMAGE'}, 'eventSourceARN': 'arn:aws:dynamodb:eu-west-1:266995720231:table/whom_identity_associations/stream/2022-06-10T20:33:18.387'}]}

# lambda_handler(event=event,context=None)