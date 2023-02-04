import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

users = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').begins_with('NAME')
)['Items']

for user in users:
    table.update_item(
        Key={
            'pk': user['pk'],
            'sk': user['sk']
        },
        UpdateExpression="set players_picked=:pp",
        ExpressionAttributeValues={
            ':pp': 0
        }
    )