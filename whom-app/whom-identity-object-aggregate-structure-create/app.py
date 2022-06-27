from re import A
import re
from xml.dom.minidom import Identified
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

        identityobjectaggregatename = event['pathParameters']['identity-object-aggregate-name']
        content_object = j.loads(event['body'])

        if identityobjectaggregatename != content_object['identity object aggregate']:
            return {
                    'statusCode':400,
                    'headers': {
                        "Access-Control-Allow-Headers" : "*",
                        "content-type": "application/json",
                        "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                        "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only POST request 
                    },
                    'body': j.dumps({
                        'result':'aggregate does not match url path'
                    })
                }            

        if 'version name' not in content_object or 'root object' not in content_object or 'framework structure' not in content_object:
            return {
                    'statusCode':400,
                    'headers': {
                        "Access-Control-Allow-Headers" : "*",
                        "content-type": "application/json",
                        "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                        "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only POST request 
                    },
                    'body': j.dumps({
                        'result':'invald message format'
                    })
                }            


        if chk_aggregate_existence(agg_name=identityobjectaggregatename)==0:
            return {
                    'statusCode':400,
                    'headers': {
                        "Access-Control-Allow-Headers" : "*",
                        "content-type": "application/json",
                        "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                        "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only POST request 
                    },
                    'body': j.dumps({
                        'result':'invalid aggregate name'
                    })
                }

        version = content_object['version name']

        if chk_structure_version(agg_name=identityobjectaggregatename,version_num=version) is not None:
            return {
                    'statusCode':400,
                    'headers': {
                        "Access-Control-Allow-Headers" : "*",
                        "content-type": "application/json",
                        "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                        "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only POST request 
                    },
                    'body': j.dumps({
                        'result':'a version with that name already exists for the aggregate. Submit another.'
                    })
                }            

        put_structure_version_and_current(agg_name=identityobjectaggregatename,version_num=version,object=content_object)

        delete_all_relations(agg_name=identityobjectaggregatename)

        # for j in enumerate_associations(structure=content_object['framework structure']):
        #     j['version'] = version
        #     j['identity_object_aggregate'] = identityobjectaggregatename

        #     put_relation(relation_object=j)

        relations = enumerate_associations(structure=content_object['framework structure'])

        for r in relations:
            r['version'] = version
            r['identity_object_aggregate'] = identityobjectaggregatename
            put_relation(relation_object=r)


        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "content-type": "application/json",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only POST request 
            },
            'body': j.dumps({
                'result':identityobjectaggregatename
            })
        }

    except Exception as e:
        b = bytes(str(e)+'/n'+str(event), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_aggregation_structure_creation_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def chk_aggregate_existence(agg_name):

    client = boto3.client('dynamodb')
    response = client.get_item(TableName='whom_identity_object_aggregates',  Key={'identity_object_aggregate':{'S':str(agg_name)}})
    if 'Item' in response:
        return 1
    else:
        return 0

def chk_structure_version(agg_name,version_num):

    bucket = os.environ['S3AGGREGATESTRUCTURE']
    s3key = f"{agg_name}/{version_num}/structure.json"

    try:
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, s3key)
        data = obj.get()['Body'].read().decode('utf-8') 
        return data
    except:
        return None

def put_structure_version_and_current(agg_name,version_num,object):

    bucket = os.environ['S3AGGREGATESTRUCTURE']
    s3key = f"{agg_name}/{version_num}/structure.json"
    currents3key = f"{agg_name}/current/structure.json"

    s3_client = boto3.client('s3')
    b = bytes(j.dumps(object), 'utf-8')
    f = io.BytesIO(b)
    s3_client.upload_fileobj(f, bucket, s3key)  
    
    ab = bytes(j.dumps(object), 'utf-8')
    af = io.BytesIO(ab)
    s3_client.upload_fileobj(af, bucket, currents3key)  

    return 1 #change this

def enumerate_associations(structure,parent=None,parentchain=None):

    for o in structure:
        if o == 'identity object':
            current_object = structure[o]
           
            if 'cardinality' in structure:
                p = {
                    'association_chain':str(parentchain) + '>' + current_object,
                    'parent': parent,
                    'child': current_object,
                    'cardinality':structure['cardinality']
                }
            else:
                p = {
                    'association_chain':str(parentchain) + '>' + current_object,
                    'parent': parent,
                    'child': current_object
                }

            yield p 
        
        elif o == 'relations':
            for r in structure[o]:
                if parentchain is None:
                    for k in enumerate_associations(r,parent=current_object,parentchain=str(parentchain)+'>'+current_object):
                        yield k
                else:    
                    for k in  enumerate_associations(r,parent=current_object,parentchain=parentchain+'>'+current_object):
                        yield k


def put_relation(relation_object):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_object_aggregates_associations')
    resp = table.put_item(Item=relation_object)       

    return resp

def full_query(table, **kwargs):
    response = table.query(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}

def fetch_relations(agg_name):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_object_aggregates_associations')
    full_items = full_query(table,IndexName="identity_object_aggregate-index",
                KeyConditionExpression=Key('identity_object_aggregate').eq(agg_name))

    return full_items

def delete_all_relations(agg_name):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_object_aggregates_associations')

    relation_set = fetch_relations(agg_name=agg_name)

    with table.batch_writer() as batch:
        for r in relation_set:
            batch.delete_item(Key={'association_chain':r['association_chain']})


