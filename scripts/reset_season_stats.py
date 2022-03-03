import boto3
from boto3.dynamodb.conditions import Key, Attr
import decimal
from utils import replace_decimals

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
# table = dynamodb.Table('users2020')
table = dynamodb.Table('XRL2021')

users = table.query(IndexName='sk-data-index', KeyConditionExpression=Key('sk').eq(
    'DETAILS') & Key('data').begins_with('NAME'))['Items']

for user in users:
  table.update_item(
    Key={'pk': user['pk'], 'sk': user['sk']},
    UpdateExpression="set stats=:stats, powerplays=:powerplays, inbox=:inbox, waiver_preferences=:waiver_preferences, provisional_drop=:provisional_drop, players_picked=:players_picked",
    ExpressionAttributeValues={
        ':stats': {
          'against': 0,
          'draws': 0,
          'for': 0,
          'losses': 0,
          'points': 0,
          'wins': 0,
        },
        ':powerplays': 3,
        ':inbox': [],
        ':waiver_preferences': [],
        ':provisional_drop': None,
        ':players_picked': 0
    }
  )

players = table.query(IndexName='sk-data-index', KeyConditionExpression=Key('sk').eq(
    'PROFILE') & Key('data').begins_with('TEAM'))['Items']

for player in players:
  table.update_item(
    Key={'pk': player['pk'], 'sk': player['sk']},
    UpdateExpression="set #D=:d, stats=:stats, scoring_stats=:scoring_stats, xrl_team=:xrl_team, position2=:position2, position3=:position3, new_position_appearances=:new_position_appearances, times_as_captain=:times_as_captain",
    ExpressionAttributeNames={
      '#D': 'data'
    },
    ExpressionAttributeValues={
        ':d': 'TEAM#None',
        ':stats': {},
        ':scoring_stats': {
          player['position']: {
            '2point_field_goals': 0,
            'concede': 0,
            'field_goals': 0,
            'involvement_try': 0,
            'mia': 0,
            'points': 0,
            'positional_try': 0,
            'send_off_deduction': 0,
            'send_offs': 0,
            'sin_bins': 0,
            'tries': 0,
          },
          'kicker': {
            'goals': 0,
            'points': 0,
          }
        },
        ':xrl_team': 'None',
        ':position2': None,
        ':position3': None,
        ':new_position_appearances': {},
        ':times_as_captain': 0,
    }
  )


stats = table.query(IndexName='sk-data-index', KeyConditionExpression=Key('sk').eq(
    'STATS#2022#1') & Key('data').begins_with('CLUB'))['Items']

with table.batch_writer() as batch:
  for appearance in stats:
    batch.delete_item(
      Key={
        'pk':appearance['pk'],
        'sk':appearance['sk'],
      }
    )

