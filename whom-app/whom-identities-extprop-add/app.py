import boto3
import os
from datetime import datetime
import io
import uuid
from boto3.dynamodb.conditions import Key
import json as j
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

def lambda_handler(event,context):

    s3_errors = os.environ['S3ERRORS']

    s3_client = boto3.client('s3')    
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:

        body = j.loads(event['body'])
        request_type = event['pathParameters']['request-type']

        if 'identity_guid' not in body:
            return {
                'statusCode':400,
                'body': j.dumps({'reason':'missing guid'}),
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only GET, POST request 
                }
            }

        if 'identity_attributes' not in body:
            return {
                'statusCode':400,
                'body': j.dumps({'reason':'missing attribute set'}),
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only GET, POST request 
                }
            }

        identity_attributes = body['identity_attributes']
        identity_guid = body['identity_guid']
        identity = fetch_identity(identity_guid=identity_guid)
        object_type = identity['identity_object_name']['S']
        is_same = CheckPropSchema(attribs_set=identity_attributes,object_type=object_type)
        if is_same == 1:
            req_guid = str(uuid.uuid4())
            s3key = f"{object_type}/{identity_guid}/{dt_string}/{req_guid}.json"
            message_content = {
                'guid':req_guid,
                's3key':s3key,
                'type':request_type
            }
            put_to_s3(content=event['body'],targets3key=s3key)
            messageid = add_sqs_message(content=j.dumps(message_content),from_identity_guid=identity_guid)
            add_request(request_guid=req_guid,identity_object_type=object_type,identity_guid=identity_guid,message_id=messageid,s3_key=s3key)
            return {
                'statusCode':200,
                'body': 
                    j.dumps({'requestguid':req_guid})
                ,
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only GET, POST request 
                }
            }
        else:
            return {
                'statusCode':400,
                'body': j.dumps({'reason':'schema does not match'}),
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only GET, POST request 
                }
            }

    except Exception as e:
        b = bytes(str(e)+'\n'+(str(event)), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_identity_extend_properties_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def put_to_s3(content,targets3key):

    s3_landing = os.environ['S3PROPLANDING']

    s3_client = boto3.client('s3')

    b = bytes(str(content), 'utf-8')
    f = io.BytesIO(b)
    s3_client.upload_fileobj(f, s3_landing, targets3key) 

def add_sqs_message(content,from_identity_guid):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomAddProp.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=from_identity_guid,
        MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid

def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}

def FetchObjectSchema(identity_object_type):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_identity_objects',
        Key={
            'identity_object_name': {'S': identity_object_type}
        }
    )    
    found = {}

    if 'Item' in response:
        found = response['Item']

    jfound = from_dynamodb_to_json(found)

    return jfound

def CheckPropSchema(attribs_set,object_type):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    same = 0
    object = FetchObjectSchema(object_type)
    schema = object['identity_object_attributes']

    schemakeys = list(schema.keys())
    attribssetkeys = list(attribs_set.keys())

    lschemakeys = sorted(schemakeys)
    lattribsetkeys = sorted(attribssetkeys)

    s3_errors = os.environ['S3ERRORS']

    if lschemakeys == lattribsetkeys:
        same = 1
    
    if same ==0:
        s3_client = boto3.client('s3')    
        b = bytes(str(lschemakeys)+'\n\n-------------------------------\n\n'+(str(lattribsetkeys)), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_identity_extend_properties_schema_mismatch.log')           

    return same

def fetch_identity(identity_guid):

    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.get_item(
        TableName='whom_identities',
        Key={
            'identity_guid': {'S': identity_guid}
        }
    )    
    found = {}

    if 'Item' in response:
        found = response['Item']

    return found

def add_request(request_guid,identity_object_type,identity_guid,message_id,s3_key):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_property_requests')

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    item = {
                'request_guid':             request_guid,
                'identity_object_type':     identity_object_type,
                'identity_guid':            identity_guid,
                'messageid':                message_id,
                's3_key':                   s3_key,
                'added_on':                 dt_string,
                'updated_on':               dt_string
                }

    resp = table.put_item(Item=item)