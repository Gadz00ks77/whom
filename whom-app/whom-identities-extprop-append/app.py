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
    s3proplanding = os.environ['S3LANDING']
    s3objlanding = os.environ['TARGET']
    s3populatedlanding = os.environ['POPUTARGE']

    s3_client = boto3.client('s3')
    
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    message = {}
    message['result']={}
    message['result']['from']={}

    try:

        for record in event['Records']:
            
            body_object = j.loads(record['body'])
            req_guid = body_object['req_guid']
            request_type = body_object['type']
            if request_type == 'rebuild':
                # create the property s3key and fetch the other items, then build
                property_identity = body_object['property_ident']
                identity_guid = body_object['identity_guid']
                identity_object_type = fetch_identity_object_type(identity_guid=identity_guid)
                property_object_type = fetch_identity_object_type(identity_guid=property_identity)
                propertys3key = f"{property_object_type}/{property_identity}/latest.json"
                
            elif request_type == 'append':
                # go ahead and build it (not too sure this is all "right")
                propertys3key = body_object['propertys3key']
                identity_object_type = propertys3key[:propertys3key.find('/')]
                remaining = propertys3key[propertys3key.find('/')+1:]
                identity_guid = remaining[:remaining.find('/')]

            actual_obj = j.loads(collect_s3_object(s3key=f'{identity_object_type}/{identity_guid}/{identity_object_type}.json',bucket=s3objlanding))

            if actual_obj is None:
                e = f'No valid identity object file at {identity_object_type}/{identity_guid}/{identity_object_type}.json'
                b = bytes(str(e)+'\n'+str(event), 'utf-8')
                f = io.BytesIO(b)
                s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_properties_append_error.log')    
                return {
                    'statusCode': 500,
                    'body': j.dumps({
                        'result':'failure',
                        'note':'check s3 error log'
                    })
                }
                
            property_set = j.loads(collect_s3_object(s3key=propertys3key,bucket=s3proplanding))

            if property_set is None:
                return {
                    'statusCode': 500,
                    'body': j.dumps({
                        'result':'failure',
                        'note':f'no valid property set for stated skey: {propertys3key}'
                    })
                }                

            searching_guid = property_set['identity_guid']
            attributes = property_set['identity_attributes']

            output = append_items(identityframework=actual_obj,tofind=searching_guid,addme=attributes)

            b = bytes(j.dumps(output), 'utf-8')
            f = io.BytesIO(b)
            s3_client.upload_fileobj(f, s3populatedlanding, f'{identity_object_type}/{identity_guid}/{identity_object_type}.json')   

        return {
            'statusCode':200,
            'body': j.dumps({
                'result':'success'
            })
        }

    except Exception as e:
        b = bytes(str(e)+'\n'+str(event), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_property_append_queue_response_error.log')    
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

def append_items(identityframework,tofind,addme):

    found = 0

    for k in identityframework:
        if k == 'identity_guid':
            if identityframework[k]==tofind:
                found = 1
        elif isinstance(identityframework[k],dict):
            append_items(identityframework=identityframework[k],tofind=tofind,addme=addme)
        elif isinstance(identityframework[k],list):
            for item in identityframework[k]:
                append_items(identityframework=item,tofind=tofind,addme=addme)
        else:
            pass    

    if found == 1:
        for a in addme:
            identityframework[a]=addme[a]

    return identityframework

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

