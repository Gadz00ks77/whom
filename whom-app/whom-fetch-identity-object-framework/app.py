import boto3
import os
from datetime import datetime
import io
import uuid
from boto3.dynamodb.conditions import Key
import json as j
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer
import copy
import ast

def lambda_handler(event,context):

    s3_errors = os.environ['S3ERRORS']
    # s3objlanding = os.environ['TARGET']

    s3_client = boto3.client('s3')    
    now = datetime.now()
    dt_string = now.strftime('%Y%m%d%H%M%S%f')[:-3]

    try:

        identity_guid = event['pathParameters']['identity-guid']
        # identity_object_type = fetch_identity_object_type(identity_guid=identity_guid)
        identity_framework = FetchIt(root_identity_guid=identity_guid)
        # s3key = f'{identity_object_type}/{identity_guid}/{identity_object_type}.json'
        
        if identity_framework == {}:
            return {
                'statusCode':400,
                'headers': {
                    'Access-Control-Allow-Headers' : '*',
                    'Access-Control-Allow-Origin': '*', #Allow from anywhere 
                    'Access-Control-Allow-Methods': 'GET, OPTIONS' # Allow only GET, POST request 
                }
            }    

        return {
            'statusCode':200,
            'headers': {
                'Access-Control-Allow-Headers' : '*',
                'Access-Control-Allow-Origin': '*', #Allow from anywhere 
                'Access-Control-Allow-Methods': 'GET, OPTIONS' # Allow only GET, POST request 
            },
            'body': j.dumps(
                identity_framework
            )
        }

    except Exception as e:
        b = bytes(str(e)+'\n'+(str(event)), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_fetch_identity_object_framework_errors.log')    
        return {
            'statusCode': 500,
            'body': j.dumps({
                'result':'failure',
                'note':'check s3 error log'
            })
        }

def fetch_identity_association(identity_guid):

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

def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}

class FullAssocSet:

    def __init__(self):
        self.data = {}

def fetch_all_associations(first_guid):

    FinalAssocSet = FullAssocSet()

    associations = {}

    current_guid_associations = fetch_identity_association(identity_guid=first_guid)
    pycurrent_guid_associations = from_dynamodb_to_json(current_guid_associations)

    FinalAssocSet.data[first_guid]=pycurrent_guid_associations['association_set']

    return FinalAssocSet.data

def recurse_identities(identity_guid,parent_guid):

    # yield identity_guid

    new_assoc = fetch_identity_association(identity_guid=identity_guid)
    clean_assoc = from_dynamodb_to_json(new_assoc)

    for assoc in clean_assoc['association_set']:
        new_guid = assoc['to_identity_guid']
        if parent_guid != new_guid:
            assoc['parent_guid']= identity_guid
            yield assoc
            for r in recurse_identities(new_guid,parent_guid=identity_guid):
                yield(r)

def DoPath(current_identity_set,parent_map,add_value,target_type):

    for map in parent_map:
        this_map = parent_map[map]
        first = this_map[0]
        remaining = this_map[1:]

        found = 0

        if 'identity_guid' in current_identity_set:
            if current_identity_set['identity_guid']==first:
                # found it
                found = 1
            else:
                # do nothing?
                pass
        else:
            for item in current_identity_set:
                if item != 'identity_guid':
                    if isinstance(item,dict):
                        DoPath(current_identity_set=item,parent_map=parent_map,add_value=add_value,target_type=target_type)    
                    else:
                        DoPath(current_identity_set=current_identity_set[item],parent_map=parent_map,add_value=add_value,target_type=target_type)

        if found == 1:
            if len(remaining)==0:
                if f'{target_type}S' in current_identity_set:
                    # new object for existing association
                    if isinstance(current_identity_set[f'{target_type}S'],dict):
                        # change target dict to list
                        copy_child = copy.deepcopy(current_identity_set[f'{target_type}S'])
                        current_identity_set[f'{target_type}S']=[]
                        current_identity_set[f'{target_type}S'].append(copy_child)
                        current_identity_set[f'{target_type}S'].append({'identity_guid':add_value})
                    elif isinstance(current_identity_set[f'{target_type}S'],list):
                        # append new target dict to list
                        current_identity_set[f'{target_type}S'].append({'identity_guid':add_value})
                elif f'{target_type}S' not in current_identity_set:
                    # create new association object
                    current_identity_set[f'{target_type}S']={'identity_guid':add_value}
            else:
                for item in current_identity_set:
                    if item != 'identity_guid':
                        DoPath(current_identity_set=current_identity_set[item],parent_map={map:remaining},add_value=add_value,target_type=target_type)

    
    return current_identity_set

def BuildPathItems(currentPathDict,newItem,newItemChannel,lastnewChannel):

    if newItemChannel in currentPathDict:
        current=copy.deepcopy(currentPathDict[newItemChannel])
        current.append(newItem)
        currentPathDict[newItemChannel]=current
    else:
        if newItemChannel > 1:
            # lastparentset = currentPathDict[lastnewChannel]
            current = copy.deepcopy(currentPathDict[lastnewChannel])
            current.append(newItem)
            currentPathDict[newItemChannel]=current
        else:
            currentPathDict[newItemChannel]=[newItem]

    return currentPathDict

def newRecurseAndPath(frameworkDict,FindParent,PreviousPathItems,currentchannel,lastchannel):

    parent_guid = FindParent

    # print(type(frameworkDict))
    if isinstance(frameworkDict,dict):
        for key in frameworkDict:
            if key == 'identity_guid':
                if frameworkDict['identity_guid']==PreviousPathItems[currentchannel][len(PreviousPathItems[currentchannel])-1]:
                    # print(currentchannel)
                    # print('identity_guid:'+frameworkDict['identity_guid'])
                    # print('pathcheck:'+PreviousPathItems[currentchannel][len(PreviousPathItems[currentchannel])-1])
                    # print('already')
                    yield PreviousPathItems
                else:
                    # print('no')
                    yield PreviousPathItems
            elif isinstance(frameworkDict[key],list):
                new_channel = currentchannel
                last_new_channel = currentchannel
                for item in frameworkDict[key]:
                        new_channel = new_channel + 1
                        PreviousPathItems=BuildPathItems(currentPathDict=PreviousPathItems,newItem=item['identity_guid'],newItemChannel=new_channel,lastnewChannel=last_new_channel)
                        getch = newRecurseAndPath(frameworkDict=item,FindParent=parent_guid,PreviousPathItems=PreviousPathItems,currentchannel=new_channel,lastchannel=last_new_channel)
                        for gc in getch:
                            yield gc
            elif frameworkDict[key]['identity_guid']==parent_guid:
                # print('yes - parent key is the same!')
                # if isinstance(frameworkDict[key],list)
                PreviousPathItems=BuildPathItems(currentPathDict=PreviousPathItems,newItem=frameworkDict[key]['identity_guid'],newItemChannel=currentchannel,lastnewChannel=lastchannel)
                yield PreviousPathItems
            # elif frameworkDict[kw]
            
            else:
                # print('no - parent key is different')
                PreviousPathItems=BuildPathItems(PreviousPathItems,frameworkDict[key]['identity_guid'],currentchannel,lastchannel)
                jeff = copy.deepcopy(frameworkDict[key])
                get = newRecurseAndPath(frameworkDict=jeff,FindParent=parent_guid,PreviousPathItems=PreviousPathItems,currentchannel=currentchannel,lastchannel=lastchannel)
                for g in get:
                    yield g

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

def FetchIt(root_identity_guid):
    
    ident = root_identity_guid
    ident_set = {'starting_ident_guid':ident}

    recursed = recurse_identities(identity_guid=ident,parent_guid='')

    identities_list = []
    identities_list.append(ident_set)

    for r in recursed:
        identities_list.append(r)

    identityFramework = {}

    root_identity = identities_list.pop(0)
    starting_type = fetch_identity_object_type(root_identity['starting_ident_guid'])
    identityFramework[starting_type]={'identity_guid':root_identity['starting_ident_guid']}

    for identity in identities_list:
        parent_guid = identity['parent_guid']
        to_add_guid = identity['to_identity_guid']
        # association_type = identity['association_type']
        my_type = fetch_identity_object_type(to_add_guid)
        # my_parents_type = fetch_identity_object_type(parent_guid)
        # print('to do: '+to_add_guid)
        paths = newRecurseAndPath(frameworkDict=identityFramework,FindParent=parent_guid,PreviousPathItems={},currentchannel=1,lastchannel=1)

        for path in paths:
            # print(path)
            valid_only = {}
            for p in path:
                if parent_guid in path[p]:
                    valid_only[p]=path[p]
            if valid_only != {}:
                DoPath(current_identity_set=identityFramework,parent_map=valid_only,add_value=to_add_guid,target_type=my_type)
    
    return identityFramework

