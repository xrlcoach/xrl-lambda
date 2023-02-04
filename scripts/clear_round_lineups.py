import boto3
from boto3.dynamodb.conditions import Key, Attr
import sys

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

round_number = sys.argv[1]
print('Clearing lineups for Round ' + str(round_number))

resp = table.query(
  IndexName='sk-data-index',
  KeyConditionExpression=Key('sk').eq('LINEUP#2022#' + str(round_number)) & Key('data').begins_with('TEAM')
)

if ('Items' in resp.keys()):
  stats = resp['Items']
  print(f'Deleting {len(stats)} records')
  for stat in stats:
    table.delete_item(
      Key={
        'pk': stat['pk'],
        'sk': stat['sk']
      }
    )

print('Task complete')
