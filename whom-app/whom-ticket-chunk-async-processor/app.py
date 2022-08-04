import boto3
import os
from datetime import datetime
import io
import uuid
from boto3.dynamodb.conditions import Key
import json as j
from botocore.exceptions import ClientError
import asyncio
import aiobotocore
from aiobotocore.session import get_session

async def send_msg(event):

    session = get_session()
    async with session.create_client('sqs') as sqs:

        for record in event['Records']:
            body = j.loads(record['body'])
            identity_object = body['identity_object']
            s3chunkguid =  body['ticket_chunk_s3key']
            reference_object = body['reference_object']
            
            request = 'MATCH'
            set_nk = 'n/a'
            if 'set_type' in body:
                set_type = body['set_type']
                if set_type == 'ADD':
                    request = 'MATCH-REF'
                    set_nk = body['set_nk']
                elif set_type == 'FIRST':
                    request = 'MATCH-REF-FIRST'
                    set_nk = body['set_nk']
            for obj in reference_object:
                    obj['identity_object'] = identity_object
                    obj['ticket_chunk_s3key'] = s3chunkguid
                    obj['set_nk'] = set_nk,
                    output_obj = {
                        'request':request,
                        'reference_object':obj
                    }

                    queue_url = get_queue_target(obj['system reference'])
                    # queue_url = 'WhomReferenceItems.fifo'
                    response = await sqs.send_message(
                        QueueUrl=queue_url,
                        MessageGroupId=obj['system reference'],
                        MessageDeduplicationId=str(uuid.uuid4()),
                        MessageBody=j.dumps(output_obj)
                    )
                    
def lambda_handler(event, context):

    s3_errors = os.environ['S3ERRORS']
    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    try:
        # push_example_delete_me(output_obj=j.dumps(event))
        asyncio.run(send_msg(event=event))


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
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_ticket_chunk_processor_error.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def get_queue_target(reference):

    str_reference = str(reference)
    last_char = str_reference[len(str_reference)-1]

    if last_char.isnumeric():
        last_char = int(last_char)
    else:
        
        letter_number = str(letter_num(char=last_char))
        last_char = letter_number[len(letter_number)-1]
        last_char = int(last_char)

    if last_char == 2:
        return 'WhomReferenceItems_Z.fifo'
    elif last_char == 3:
        return 'WhomReferenceItems_Y.fifo'
    elif last_char == 4:
        return 'WhomReferenceItems_X.fifo'
    elif last_char == 5:
        return 'WhomReferenceItems_A.fifo'
    elif last_char == 6:
        return 'WhomReferenceItems_B.fifo'
    elif last_char == 7:
        return 'WhomReferenceItems_C.fifo'
    elif last_char == 8:
        return 'WhomReferenceItems_D.fifo'
    elif last_char == 9:
        return 'WhomReferenceItems_E.fifo'
    else:
        return 'WhomReferenceItems.fifo'

def letter_num(char):

    return ord(char.upper())


def push_example_delete_me(output_obj):

    s3_errors = os.environ['S3ERRORS']
    s3_client = boto3.client('s3')
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S%f")[:-3]

    b = bytes(j.dumps(output_obj), 'utf-8')
    f = io.BytesIO(b)
    s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_ticket_chunk_processor_example.log')   

    return 0 