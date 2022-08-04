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

    try:

        for record in event['Records']:
            
            body_object = j.loads(record['body'])
            reference_object = body_object['object']
            system_reference = reference_object['system reference']
            source = reference_object['source']
            ticket_chunk_s3key = reference_object['ticket_chunk_s3key']
            identity_guid = reference_object['identity_guid']

            modify_identity(reference=system_reference,source_ticket='tbc',source=source,identity_guid=identity_guid)
            message = {'outcome':'success','object':reference_object,'reason':'modified reference and identity objects'}
            add_sqs_message(j.dumps(message),ticket_chunk_s3key)
                    
        # outfile(system_reference=j.dumps(message)+'\n\n'+str(event),s3_errors=s3_errors,s3_client=s3_client,timestring=dt_string)

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
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_modify_identity_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def modify_identity(reference,source,source_ticket,identity_guid):

    new_identity_chain = [
            {
                'source':source,
                'system_reference':reference,
                'source_ticket_guid':source_ticket
        }
        ]
    
    table = boto3.resource('dynamodb').Table('whom_identities')
    
    response = table.update_item(
        Key={
            'identity_guid':identity_guid
        },
        UpdateExpression="set identity_chain = list_append(identity_chain,:vals)",
        ExpressionAttributeValues={
            ':vals': new_identity_chain,
        },
        ReturnValues="UPDATED_NEW"
    )

    return response


def add_sqs_message(content,s3chunkkey):

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WhomReturns')
    response = queue.send_message(
        MessageBody=content
        # MessageGroupId=s3chunkkey,
        # MessageDeduplicationId=str(uuid.uuid4()))
    )
    messageid = response.get('MessageId')
    
    return messageid

def outfile(system_reference,s3_errors,s3_client,timestring):
    
    b = bytes(str('success\n'+system_reference), 'utf-8')
    f = io.BytesIO(b)
    s3_client.upload_fileobj(f, s3_errors, f'whom_{timestring}_modify_identity.log')

    return 0    

# event = {'Records': [{'messageId': '17be82f4-d32b-4c03-a463-0c9f42955ad0', 'receiptHandle': 'AQEBkiCHr4dbs4JC36Bon64LT7ZpHx9V3g4s8dM1I+aA91OJ32Sj6iXEaTdhz7uR6M+MOHD0MvMt6EhkNXGpcJvLJe/3MW64J/2cGKHD2/2muoq+G6rUZyAkoWA4T2h01prEFKIU53XBljSqKbq+mkWWqKAWHEUFV6cjGRWy7WxX9Q5u2/LMv/xtKsYgKrNpKE2Ty3qoXyDNw7MHWwYQ6WuriKk0MNKD1Wl0snLq+NcylJ+2vzDJlISrv/oTobUMfU4tx5PAEhgwmVqVSRLq5CCsJODNJA1HAA7LsIy4HTErc8k=', 'body': '{"outcome": "success", "object": {"system reference": "L1315CWSDWS", "source": "SYSTEM D", "identity guid": "3e49b36b-559c-4cdf-a2eb-ba1bfafcc1a9", "identity_object": "LAYER", "ticket_chunk_s3key": "7a0b01c0-af79-4115-a022-7d0aea55e3f5/identity_object_e53dff13-fa9c-4874-a9d0-2c37f61cd5c9.json", "identity_guid": "3e49b36b-559c-4cdf-a2eb-ba1bfafcc1a9"}, "reason": "new reference, added to modify identity queue"}', 'attributes': {'ApproximateReceiveCount': '1', 'SentTimestamp': '1652714721138', 'SequenceNumber': '18869839042320880128', 'MessageGroupId': '3e49b36b-559c-4cdf-a2eb-ba1bfafcc1a9', 'SenderId': 'AROAT4KRPMAT5X6Z5QVM7:whom-app-whomGenReferences-UTVi1f3FVdZ6', 'MessageDeduplicationId': '798531ff-6a82-479c-a672-417cc033b632', 'ApproximateFirstReceiveTimestamp': '1652714721138'}, 'messageAttributes': {}, 'md5OfBody': 'a993fb06da00bd9afd792e61fa7bc55d', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:eu-west-1:266995720231:WhomIdentitiesItems.fifo', 'awsRegion': 'eu-west-1'}]}

# lambda_handler(event,context=None)