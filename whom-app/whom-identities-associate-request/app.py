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

    s3_client = boto3.client('s3')    
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:
        associate_object = j.loads(event['body'])
        from_identity_guid = associate_object['from identity guid']
        to_identity_guid = associate_object['to identity guid']
        if 'aggregate' in associate_object:
            aggregate = associate_object['aggregate']
        else:
            aggregate = None
        
        if 'aggregate version' in associate_object:
            version = associate_object['aggregate version'] 
        else:
            version = None
        
        from_identity_object = fetch_identity(identity_guid=from_identity_guid)
        to_identity_object = fetch_identity(identity_guid=to_identity_guid)

        if from_identity_object == {} or to_identity_object == {}:
            return {
                'statusCode':400,
                'headers': {
                    "Access-Control-Allow-Headers" : "*",
                    "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                    "Access-Control-Allow-Methods": "POST, OPTIONS" 
                }
            }    

        if aggregate is not None:
            if chk_association(agg_ame=aggregate,from_obj=from_identity_object,to_obj=to_identity_object) == 0:
                return {
                    'statusCode':400,
                    'headers': {
                        "Access-Control-Allow-Headers" : "*",
                        "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                        "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only GET, POST request 
                    },
                    'body': j.dumps({
                        'outcome':'inappropriate association'
                    })
                }

        association_ticket_guid = str(uuid.uuid4())
        message = {
            'association_ticket_guid': association_ticket_guid,
            'associate_object': associate_object

        }

        add_sqs_message(content=j.dumps(message),from_identity_guid=from_identity_guid)

        return {
            'statusCode':200,
            'headers': {
                "Access-Control-Allow-Headers" : "*",
                "Access-Control-Allow-Origin": "*", #Allow from anywhere 
                "Access-Control-Allow-Methods": "POST, OPTIONS" # Allow only GET, POST request 
            },
            'body': j.dumps({
                'outcome':'association request queued',
                'association_ticket_guid': association_ticket_guid
            })
        }

    except Exception as e:
        b = bytes(str(e)+'\n'+(str(event)), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_associate_identities_errors.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

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

def add_sqs_message(content,from_identity_guid):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomAssociations.fifo')
    response = queue.send_message(
        MessageBody=content,
        MessageGroupId=from_identity_guid,
        MessageDeduplicationId=str(uuid.uuid4()))
    messageid = response.get('MessageId')
    
    return messageid

def full_query(table, **kwargs):
    response = table.query(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

def fetch_relations(agg_name):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('whom_identity_object_aggregates_associations')
    full_items = full_query(table,IndexName="identity_object_aggregate-index",
                KeyConditionExpression=Key('identity_object_aggregate').eq(agg_name))

    return full_items


def chk_association(agg_ame,from_obj,to_obj):

    all_relations_for_aggregate = fetch_relations(agg_name=agg_ame)

    found = 0

    for r in all_relations_for_aggregate:
        if from_obj == r['parent'] and to_obj == r['child']:
            found = 1

    return found
