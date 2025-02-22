import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

squads = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
)['Items']

with table.batch_writer() as batch:
  for player in squads:
    split = player['player_name'].split(' ')
    first_name = split[0]
    last_name = ' '.join(split[1:])
    last_name = last_name.upper()
    updated_name = ' '.join([first_name, last_name])
    table.update_item(
      Key={
          'pk': player['pk'],
          'sk': player['sk']
      },
      UpdateExpression="set player_name=:n",
      ExpressionAttributeValues={
          ':n': updated_name
      }
    )