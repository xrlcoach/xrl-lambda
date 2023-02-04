from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr

CURRENT_YEAR = 2022

dynamodbResource = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodbResource.Table('XRL2021')
# all_stats = stats_table.scan()['Items']
players = table.query(
  IndexName='sk-data-index',
  KeyConditionExpression=Key('sk').eq(
      'PROFILE') & Key('data').begins_with('TEAM'),
  FilterExpression=Attr('new_position_appearances.Back').gt(1) | Attr('new_position_appearances.Forward').gt(1) | Attr('new_position_appearances.Playmaker').gt(1)
)['Items']
print(len(players))