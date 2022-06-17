import boto3
import os
from datetime import datetime
import io
import uuid
import urllib.parse
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

        for r in event['Records']:
            bucket = r['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(r['s3']['object']['key'], encoding='utf-8')
            name_array = key.split('/')
            identity_guid = name_array[1]

            obj = collect_s3_object(s3key=key,bucket=bucket)

            if obj!= None:
                for k in all_object_list(framework=j.loads(obj)):
                    message_content = {
                        'req_guid': str(uuid.uuid4()),
                        'type':'rebuild',
                        'identity_guid':identity_guid,
                        'property_ident': k
                    }
                    messageid = add_sqs_message(content=j.dumps(message_content),to_populate=k,from_identity_guid=identity_guid)

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