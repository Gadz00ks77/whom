from urllib import response
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
    s3proplanding = os.environ['S3PROPLANDING']
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
                pass

            current_hash_record = get_hash_record(identity_guid=identity_guid)
            current_object_hash = current_hash_record['object_hash']
            populated_object_hash = current_hash_record['populated_hash']
            completed_count = current_hash_record['completed_count']
            object_count = current_hash_record['object_count']

            if populated_object_hash != current_object_hash and completed_count == 0:
                actual_obj = j.loads(collect_s3_object(s3key=f'{identity_object_type}/{identity_guid}/{identity_object_type}.json',bucket=s3objlanding))
            elif populated_object_hash == current_object_hash and completed_count != 0:
                actual_obj = j.loads(collect_s3_object(s3key=f'{identity_object_type}/{identity_guid}/{identity_object_type}.json',bucket=s3populatedlanding))
            elif populated_object_hash == current_object_hash and completed_count == 0:
                actual_obj = j.loads(collect_s3_object(s3key=f'{identity_object_type}/{identity_guid}/{identity_object_type}.json',bucket=s3populatedlanding))
            else:
                pass #this needs fixing as I'm not sure it's a valid outcome. 4 ifs with two outcomes. Later.

            property_response = collect_s3_object(s3key=propertys3key,bucket=s3proplanding)

            if property_response is None:
                output = actual_obj
            else:
                property_set = j.loads(property_response)
                searching_guid = property_set['identity_guid']
                attributes = property_set['identity_attributes']
            
                output = append_items(identityframework=actual_obj,tofind=searching_guid,addme=attributes)

            b = bytes(j.dumps(output), 'utf-8')
            f = io.BytesIO(b)
            s3_client.upload_fileobj(f, s3populatedlanding, f'{identity_object_type}/{identity_guid}/{identity_object_type}.json')   

            if completed_count + 1 >= object_count:
                new_status = 'built'
            else:
                new_status = 'in progress'
            
            update_hash_record(identity_guid=identity_guid,new_populated_hash=current_object_hash,new_completed_count=completed_count + 1,new_status=new_status)

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


def get_hash_record(identity_guid):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_framework_hash')

    response = table.get_item(
        Key={
            'identity_guid':identity_guid
        }
    )

    return response['Item']

def update_hash_record(identity_guid,new_populated_hash,new_completed_count,new_status):

    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_framework_hash')

    response = table.update_item(
        Key={
            'identity_guid':identity_guid
        },
        UpdateExpression="set completed_count=:cc,populated_hash=:ph,lastupdate_datetime=:id,identity_hash_status=:is",
        ExpressionAttributeValues={
            ':ph': new_populated_hash,
            ':cc': new_completed_count,
            ':id': dt_string,
            ':is': new_status
        },
        ReturnValues="UPDATED_NEW"
    )

    return response

