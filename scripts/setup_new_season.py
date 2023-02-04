import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

CURRENT_YEAR = 2023

print(f"Setting up XRL Year {CURRENT_YEAR}")

players = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
)['Items']

print("")

for player in players:
  if player['stats']:
    table.put_item(
        Item={
            'pk': player['pk'],
            'sk': f'YEARSTATS#{CURRENT_YEAR - 1}',
            'data': 'PLAYER_NAME#' + player['player_name'],
            'player_name': player['player_name'],
            'search_name': player['search_name'],
            'year': CURRENT_YEAR - 1,
            'stats': player['stats'],
            'scoring_stats': player['scoring_stats'],
        }
    )

users = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').begins_with('NAME#')
)['Items']

for user in users:
  table.put_item(
    Item={
      'pk': user['pk'],
      'sk': f'YEARSTATS#{CURRENT_YEAR - 1}',
      'data': f'TEAM#{user["team_short"]}',
      'username': user['username'],
      'stats': user['stats'],
      'year': CURRENT_YEAR - 1,
    }
  )

for i in range (1, 22):
  table.put_item(
      Item={
          'pk': f'ROUND#{CURRENT_YEAR}#' + str(i),
          'sk': 'STATUS',
          'data': 'ACTIVE#' + 'true' if i == 1 else 'false',
          'active': True if i == 1 else False,
          'scooping': True if i == 1 else False,
          'in_progress': False,
          'completed': False,
          'round_number': i,
          'year': CURRENT_YEAR
      }
  )