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

    message = {}
    message['result']={}
    message['result']['from']={}

    try:

        for record in event['Records']:
            
            body_object = j.loads(record['body'])
            associate_object = body_object['associate_object']
            from_identity_guid = associate_object['from identity guid']
            to_identity_guid = associate_object['to identity guid']
            cardinality = associate_object['cardinality']
            association_type = associate_object['association type']

            from_set = fetch_association(from_identity_guid)
            to_set = fetch_association(to_identity_guid)

            if from_set != {}:
                found_set_from = 1
                found_from = chk_association_exists(to_identity_guid=to_identity_guid,association_set=from_set,type=association_type,cardinality=cardinality)
            else:
                found_set_from = 0
                found_from = 0
            
            if to_set != {}:
                found_set_to = 1
                found_to = chk_association_exists(to_identity_guid=from_identity_guid,association_set=to_set,type=association_type,cardinality=cardinality)
            else:
                found_set_to = 0
                found_to = 0

            if found_from == 1:
                message['result']['from']=='found an existing association for from the specified guid to the target guid for the specified type and cardinality'
            else:
                message['result']['from']=='new association recorded'
                add_association(exists_marker=found_set_from,from_identity_guid=from_identity_guid,to_identity_guid=to_identity_guid,association_type=association_type,cardinality=cardinality,parent=1)

            if found_to == 1:
                message['result']['from']=='found an existing association for from the specified target guid to the "from" guid for the specified type and cardinality'
            else:
                message['result']['from']=='new association recorded'
                add_association(exists_marker=found_set_to,from_identity_guid=to_identity_guid,to_identity_guid=from_identity_guid,association_type=association_type,cardinality=cardinality[::-1],parent=0)

        b = bytes(str(event), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_associate_identity_process.log')   

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

def fetch_association(identity_guid):

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

def chk_association_exists(to_identity_guid,association_set,type,cardinality):

    found = 0

    print(association_set)

    for association in association_set['association_set']['L']:
        
        if association['M']['association_type']['S']==type and association['M']['cardinality']['S']==cardinality and association['M']['to_identity_guid']['S']==to_identity_guid:
            found = 1

    return found

def add_association(exists_marker,from_identity_guid,to_identity_guid,association_type,cardinality,parent):

    table = boto3.resource('dynamodb').Table('whom_identity_associations')

    if parent==1:
        is_parent='Parent'
    else:
        is_parent='Child'

    new_association = [
            {
                'to_identity_guid':to_identity_guid,
                'association_type':association_type,
                'cardinality':cardinality,
                'parent':is_parent
        }
        ]

    if exists_marker == 1:
        
        response = table.update_item(
            Key={
                'identity_guid':from_identity_guid
            },
            UpdateExpression="set association_set = list_append(association_set,:vals)",
            ExpressionAttributeValues={
                ':vals': new_association,
            },
            ReturnValues="UPDATED_NEW"
        )
    else:

        assoc_item = {
        'identity_guid':    from_identity_guid,
        'association_set':  new_association
        }

        response = table.put_item(Item=assoc_item)

    return response

# event = {'Records': [
#     {'messageId': '0d29bb86-69ec-498b-a179-fa3a130989cb', 
#     'receiptHandle': 'AQEB4bbN4y0A4Us+O6EmiBqIEzQl+WuVuK28r7ne+f5Sq32MePLQbOEJ6yuwAVpxIrkqmVraDNxs4u/fp2uU+xcnFpgH2PD6JtqJtGOml4YSC0YnrOxozsqPMJBAuPusFdkrPt/j4/UU2yE85wO3s6bT0R5cVzmb5v1dlVrp8AYWpxa88BV76QKqvLV6ATKe/69pH+BP563GAvgLdCFdLbnfyC0Nv9Glme4cIZg1tHFcRY8iFgGIZh6QjBrag7yvqIl4qWXNJn4v4scHC/RHWF5YZQv9qXtPAyuSafD6X+R0FnM=', 
#     'body': '{"association_ticket_guid": "cee45cc0-1c69-456d-aff4-5a890193af46", "associate_object": {"from identity guid": "12303cf7-3561-4452-9856-f5381b131d59", "to identity guid": "9178226c-51a9-4dd6-b68f-2198bc38bb95", "association type": "HAS", "cardinality": "1:?"}}', 'attributes': {'ApproximateReceiveCount': '1', 'SentTimestamp': '1653661034042', 'SequenceNumber': '18870081298424304384', 'MessageGroupId': '12303cf7-3561-4452-9856-f5381b131d59', 'SenderId': 'AROAT4KRPMAT5X6Z5QVM7:whom-app-whomAssociateRequest-OBzdzye1J8F7', 'MessageDeduplicationId': 'a8a9b402-5199-4c88-8f88-a43bebb5694e', 'ApproximateFirstReceiveTimestamp': '1653661034042'}, 'messageAttributes': {}, 'md5OfBody': 'caefcac8fd885fd534538f3c56a16d9d', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:eu-west-1:266995720231:WhomAssociations.fifo', 'awsRegion': 'eu-west-1'}]}

# lambda_handler(event,context=None)