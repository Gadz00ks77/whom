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

    s3_client = boto3.client('s3')    
    now = datetime.now()
    dt_string = now.strftime('%Y%m%d%H%M%S%f')[:-3]

    try:

        identity_guid = event['pathParameters']['identity-guid']
        identity_framework = FetchIt(root_identity_guid=identity_guid)
        populated_framework = AaaaaandPopulate(populatedFramework=identity_framework)

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
                populated_framework
            )
        }

    except Exception as e:
        b = bytes(str(e)+'\n'+(str(event)), 'utf-8')
        f = io.BytesIO(b)
        s3_client.upload_fileobj(f, s3_errors, f'whom_{dt_string}_fetch_identity_associations.log')    
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

class FullAssocSet:

    def __init__(self):
        self.data = {}

def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}

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

def CWsetviaPath(identity_set,parent_map,add_value):

    for mapnum in parent_map:
        map_chain = parent_map[mapnum]
        print(map_chain)

        first_item = map_chain[0]
        remaining = map_chain[1:]

        for ident in identity_set:
            if isinstance(identity_set[ident],dict):
                print('found dict')
                if ident==first_item:
                    print('map match')
                    if len(remaining)>=1:
                        # next item please
                        CWsetviaPath(identity_set=identity_set[ident],parent_map={mapnum:remaining},add_value=add_value)
                    else:
                        # no more items to check so add the new dict
                        if identity_set[ident]=={}:
                            identity_set[ident]={add_value:{}}
                        else:
                            new_list = []
                            current=copy.deepcopy(identity_set[ident])
                            new_list.append(current)
                            new_list.append({add_value:{}})
                            identity_set[ident]=new_list

            elif isinstance(identity_set[ident],list):
                print('found list')
                for listed_items in identity_set[ident]:
                    CWsetviaPath(identity_set=listed_items,parent_map={mapnum:remaining},add_value=add_value)

            else:
                pass
     
    return identity_set

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

def RecurseAndPath(strDict,FindParent,PreviousPathItems,currentchannel=None,lastchannel=None):

    newDict = ast.literal_eval(strDict)

    if isinstance(newDict,dict):
        for identkey in newDict:
            
            if identkey == FindParent:
                if isinstance(newDict[identkey],list):
                    # PreviousPathItems.append(identkey)
                    PreviousPathItems=BuildPathItems(PreviousPathItems,identkey,currentchannel,lastchannel)
                    for item in newDict[identkey]:
                        ldict=copy.deepcopy(item)
                        getittoo = RecurseAndPath(strDict=str(ldict),FindParent=FindParent,PreviousPathItems=PreviousPathItems,currentchannel=currentchannel,lastchannel=lastchannel)
                        for gt in getittoo:
                            yield gt
                else:
                    # PreviousPathItems.append(identkey)
                    PreviousPathItems=BuildPathItems(PreviousPathItems,identkey,currentchannel,lastchannel)
                    yield PreviousPathItems
            elif isinstance(identkey,list):
                for child in identkey:
                    # PreviousPathItems.append(child)
                    PreviousPathItems=BuildPathItems(PreviousPathItems,child,currentchannel,lastchannel)
                    getlistit = RecurseAndPath(strDict=str(child),FindParent=FindParent,PreviousPathItems=PreviousPathItems,currentchannel=currentchannel,lastchannel=lastchannel)
                    for glt in getlistit:
                        yield glt
            elif isinstance(identkey,dict):
                getdictit = RecurseAndPath(strDict=str(identkey),FindParent=FindParent,PreviousPathItems=PreviousPathItems,currentchannel=currentchannel,lastchannel=lastchannel)
                for gdt in getdictit:
                    yield gdt
            else:
                # PreviousPathItems.append(identkey)
                PreviousPathItems=BuildPathItems(PreviousPathItems,identkey,currentchannel,lastchannel)
                jeff = copy.deepcopy(newDict[identkey])
                # print('jeff'+str(jeff))
                get = RecurseAndPath(strDict=str(jeff),FindParent=FindParent,PreviousPathItems=PreviousPathItems,currentchannel=currentchannel,lastchannel=lastchannel)
                for g in get:
                    yield g
    else:
        new_channel = currentchannel
        last_new_channel = currentchannel
        for item in newDict:
            new_channel = new_channel + 1
            getch = RecurseAndPath(strDict=str(item),FindParent=FindParent,PreviousPathItems=PreviousPathItems,currentchannel=new_channel,lastchannel=last_new_channel)
            for gc in getch:
                yield gc

def FetchIt(root_identity_guid):
    ident = root_identity_guid
    # ident = '9178226c-51a9-4dd6-b68f-2198bc38bb95'
    ident_set = {'starting_ident_guid':ident}

    recursed = recurse_identities(identity_guid=ident,parent_guid='')

    identities_list = []
    identities_list.append(ident_set)

    for r in recursed:
        identities_list.append(r)

    identityFramework = {}

    root_identity = identities_list.pop(0)
    identityFramework[root_identity['starting_ident_guid']]={}

    for identity in identities_list:
        parent_guid = identity['parent_guid']
        to_add_guid = identity['to_identity_guid']
        # print(to_add_guid)
        # print(parent_guid)
        paths = RecurseAndPath(strDict=str(identityFramework),FindParent=parent_guid,PreviousPathItems={},currentchannel=1,lastchannel=1)

        for path in paths:
            valid_only = {}
            for p in path:
                if parent_guid in path[p]:
                    valid_only[p]=path[p]
            # print(path)
            CWsetviaPath(identity_set=identityFramework,parent_map=valid_only,add_value=to_add_guid)
    
    return identityFramework

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

def AaaaaandPopulate(populatedFramework):

    for obj in populatedFramework:
        if isinstance(populatedFramework[obj],dict):
            that_dict = copy.deepcopy(populatedFramework[obj])
            populatedFramework[obj]={}
            populatedFramework[obj]['object type']=fetch_identity_object_type(obj)
            populatedFramework[obj]['association']=that_dict
            AaaaaandPopulate(populatedFramework=populatedFramework[obj]['association'])
        elif isinstance(populatedFramework[obj],list):
            that_list = populatedFramework[obj]
            populatedFramework[obj]={}
            populatedFramework[obj]['object type']=fetch_identity_object_type(obj)
            populatedFramework[obj]['association set']=that_list
            for children in populatedFramework[obj]['association set']:
                AaaaaandPopulate(populatedFramework=children)

    return populatedFramework
