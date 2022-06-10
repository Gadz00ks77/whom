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
            s3key = body_object['s3key']
            request_type = body_object['type'] #not being used at the mo. Later.

            identity_object_type = s3key[:s3key.find('/')]
            remaining = s3key[s3key.find('/')+1:]
            identity_guid = remaining[:remaining.find('/')]
            actual_obj = j.loads(collect_s3_object(s3key=f'{identity_object_type}/{identity_guid}/{identity_object_type}.json',bucket=s3objlanding))

            if actual_obj is None:
                pass
                
            property_set = j.loads(collect_s3_object(s3key=s3key,bucket=s3proplanding))
            searching_guid = property_set['identity_guid']
            attributes = property_set['identity_attributes']

            output = append_items(identityframework=actual_obj,tofind=searching_guid,addme=attributes)

            b = bytes(str(output), 'utf-8')
            f = io.BytesIO(b)
            s3_client.upload_fileobj(f, s3objlanding, f'{identity_object_type}/{identity_guid}/{identity_object_type}.json')   

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
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_associate_identity_process_error.log')    
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
