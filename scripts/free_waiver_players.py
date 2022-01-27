import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

on_waivers = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').eq('TEAM#On Waivers')
)['Items']
#Update each of those players to have XRL team of 'None'
for player in on_waivers:
    table.update_item(
        Key={
            'pk': player['pk'],
            'sk': player['sk']
        },
        UpdateExpression="set #D=:d, xrl_team=:n",
        ExpressionAttributeNames={
            '#D': 'data'
        },
        ExpressionAttributeValues={
            ':d': 'TEAM#None',
            ':n': 'None' 
        }
    )