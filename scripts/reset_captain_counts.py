import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

captains = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM'),
    FilterExpression=Attr('times_as_captain').gt(0)
)['Items']

for player in captains:
  table.update_item(
    Key={
      'pk': player['pk'],
      'sk': player['sk']
    },
    UpdateExpression="set times_as_captain=:t",
    ExpressionAttributeValues={
        ':t': 0
    }
  )