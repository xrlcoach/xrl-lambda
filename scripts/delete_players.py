import boto3
from boto3.dynamodb.conditions import Key, Attr
import csv

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

players_to_delete = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM'),
    FilterExpression=Attr('player_id').gte('100807')
)['Items']

print(f"Found {len(players_to_delete)} players to delete");

with table.batch_writer() as batch:
    for player in players_to_delete:
        batch.delete_item(Key={
            "pk": player['pk'],
            "sk": player['sk'] 
        })