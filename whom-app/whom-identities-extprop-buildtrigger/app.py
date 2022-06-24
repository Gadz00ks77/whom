import boto3
import os
from datetime import datetime
import io
import uuid
import urllib.parse
from boto3.dynamodb.conditions import Key
import json as j
from botocore.exceptions import ClientError
import hashlib

def lambda_handler(event,context):

    s3_errors = os.environ['S3ERRORS']

    s3_client = boto3.client('s3')
    # dynamodb = boto3.resource('dynamodb')
    # table = dynamodb.Table('whom_ticket')
    
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:

        for r in event['Records']:
            bucket = r['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(r['s3']['object']['key'], encoding='utf-8')
            name_array = key.split('/')
            identity_guid = name_array[1]

            obj = collect_s3_object(s3key=key,bucket=bucket)
            hash_object = hashlib.sha256(obj.encode('utf-8'))
            hex_dig = hash_object.hexdigest()


            message_package = []

            if obj!= None:
                cnt = 0
                for k in all_object_list(framework=j.loads(obj)):
                    cnt = cnt + 1
                    message_content = {
                        'req_guid': str(uuid.uuid4()),
                        'type':'rebuild',
                        'identity_guid':identity_guid,
                        'property_ident': k
                    }
                    message_package.append(message_content)

            upsert_hash_record(identity_guid=identity_guid,object_hash=hex_dig,object_count=cnt)

            for p in message_package:        
                messageid = add_sqs_message(content=j.dumps(p),to_populate=p['property_ident'],from_identity_guid=p['identity_guid'])

        b = bytes(str(event), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_extprop_buildtrigger.log')   

        return {
            'statusCode':200,
            'body': j.dumps({
                'result':'success'
            })
        }

    except Exception as e:
        b = bytes(str(e), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_extprop_buildtrigger_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def collect_s3_object(s3key,bucket):

    try:
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, s3key)
        data = obj.get()['Body'].read().decode('utf-8') 
        return data
    except:
        return None

def add_sqs_message(content,to_populate,from_identity_guid):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomPopulate.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=from_identity_guid,
        MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid

def all_object_list(framework):

    for o in framework:
        if o == 'identity_guid':
            yield framework[o]
        elif isinstance(framework[o],dict):
            for k in all_object_list(framework[o]):
                yield k
        elif isinstance(framework[o],list):
            for l in framework[o]:
                for k  in all_object_list(l):
                    yield k

def upsert_hash_record(identity_guid,object_hash,object_count):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    dynamodb = boto3.resource('dynamodb')
    try:
        table = dynamodb.Table('whom_identity_framework_hash')
        response = table.put_item(
        
        Item={
                'identity_guid':identity_guid,
                'object_hash': object_hash,
                'populated_hash': 'none',
                'object_count': object_count,
                'completed_count': 0,
                'insert_datetime': dt_string,
                'lastupdate_datetime': dt_string,
                'identity_hash_status':'triggered'
            },
        ConditionExpression='attribute_not_exists(identity_guid)'
        )

        return response

    except ClientError as e:
        # Ignore the ConditionalCheckFailedException
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            response = table.update_item(
                Key={
                    'identity_guid':identity_guid
                },
                UpdateExpression="set completed_count=:cc,object_hash=:om, object_count=:oc,insert_datetime=:id,lastupdate_datetime=:id,identity_hash_status=:is",
                ExpressionAttributeValues={
                    ':om': object_hash,
                    ':oc': object_count,
                    ':id': dt_string,
                    ':is': 'triggered',
                    ':cc': 0
                },
                ReturnValues="UPDATED_NEW"
            )

            return response

        else:
            raise


