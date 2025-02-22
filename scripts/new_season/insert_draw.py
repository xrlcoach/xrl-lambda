import csv

import boto3
from boto3.dynamodb.conditions import Attr, Key

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

CURRENT_YEAR = 2025

for i in range(1, 23):
    resp = table.query(
        KeyConditionExpression=Key('pk').eq(f'ROUND#{CURRENT_YEAR}#' + str(i)) & Key('sk').begins_with('FIXTURE')
    )
    if 'Items' in resp.keys():
      fixtures = resp['Items']
      for match in fixtures:
          table.delete_item(
              Key={
                  'pk': match['pk'],
                  'sk': match['sk']
              }
          )

with open('data/XRL Schedule 2025.csv', 'r') as fixtures:
    reader = csv.reader(fixtures)
    round_number = 0
    for row in reader:
        if row[0].isnumeric():
            round_number = int(row[0])
            home = row[1]
            away = row[2]  
            print(f'Round {round_number} {home} v {away}')
            table.put_item(Item={
                'pk': f'ROUND#{CURRENT_YEAR}#' + str(round_number),
                'sk': 'FIXTURE#' + home + '#' + away,
                'year': CURRENT_YEAR,
                'data': 'COMPLETED#false',
                'home': home,
                'away': away,
                'home_score': 0,
                'away_score': 0,
            })