import boto3
from boto3.dynamodb.conditions import Key, Attr
import csv
from data.xrl_teams import team_names

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

for i in range(1, 22):
    resp = table.query(
        KeyConditionExpression=Key('pk').eq('ROUND#2022#' + str(i)) & Key('sk').begins_with('FIXTURE')
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

with open('data/XRL Schedule 2022 - Fixtures.csv', 'r') as fixtures:
    reader = csv.reader(fixtures)
    round_number = 0
    for row in reader:
        if row[1] != '':
            if row[1].startswith('ROUND'):
                round_number = int(row[1].split()[1])
                print(round_number)
        if row[2] != '' and row[4] == 'v':
            home_team_name = row[2]
            home = team_names[home_team_name]
            away_team_name = row[6]  
            away = team_names[away_team_name]
            print(f'{home} v {away}')
            table.put_item(Item={
                'pk': 'ROUND#2022#' + str(round_number),
                'sk': 'FIXTURE#' + home + '#' + away,
                'year': 2022,
                'data': 'COMPLETED#false',
                'home': home,
                'away': away,
                'home_score': 0,
                'away_score': 0,
            })