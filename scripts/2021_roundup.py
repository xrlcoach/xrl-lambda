import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import decimal
import hashlib
import base64


def replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj


dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
# table = dynamodb.Table('users2020')
table = dynamodb.Table('XRL2021')

for round_number in range(1, 26):
  resp = table.query(
      #IndexName='sk-data-index',
      KeyConditionExpression=Key('pk').eq(
          'WAIVER' + str(round_number)) & Key('sk').begins_with('REPORT#')
  )
  for r in replace_decimals(resp['Items']):
    table.update_item(
          Key={
              'pk': r['pk'],
              'sk': r['sk']
          },
          UpdateExpression="set #Y=:y",
          ExpressionAttributeNames={
              '#Y': 'year'
          },
          ExpressionAttributeValues={
              ':y': 2021
          }
      )





# for player in players:
#     table.put_item(
#         Item={
#             'pk': player['pk'],
#             'sk': 'YEARSTATS#2021',
#             'data': 'PLAYER_NAME#' + player['player_name'],
#             'player_name': player['player_name'],
#             'search_name': player['search_name'],
#             'year': 2021,
#             'stats': player['stats'],
#             'scoring_stats': player['scoring_stats'],
#         }
#     )

# for user in users:
#   table.put_item(
#     Item={
#       'pk': user['pk'],
#       'sk': 'YEARSTATS#2021',
#       'data': f'TEAM#{user["team_short"]}',
#       'username': user['username'],
#       'stats': user['stats'],
#       'year': 2021,
#     }
#   )
