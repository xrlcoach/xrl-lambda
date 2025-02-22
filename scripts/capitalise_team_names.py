import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

round_lineups = table.query(
  IndexName='sk-data-index',
  KeyConditionExpression=Key('sk').eq(f'LINEUP#2023#17') & Key('data').begins_with('TEAM#')
)['Items']

for player in round_lineups:

  # table.update_item(
  #   Key={
  #     'pk': player['pk'],
  #     'sk': player['sk']
  #   },
  #   UpdateExpression="set nrl_club=:c",
  #   ExpressionAttributeValues={
  #       ':c': player['nrl_club'].title()
  #   }
  # )

  table.update_item(
    Key={
        'pk': player['pk'],
        'sk': player['sk']
    },
    UpdateExpression="set nrl_club=:c",
    ExpressionAttributeValues={
        ':c': player['nrl_club'].title()
    }
  )