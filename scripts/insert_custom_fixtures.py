import boto3
from boto3.dynamodb.conditions import Attr, Key

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

CURRENT_YEAR = 2025

ROUND_NUMBER = 26

fixtures = [
    ['BOX','WOL'],
]

print(fixtures)

for fixture in fixtures:
    home = fixture[0]
    away = fixture[1]
    table.put_item(Item={
        'pk': f'ROUND#{CURRENT_YEAR}#{ROUND_NUMBER}',
        'sk': 'FIXTURE#' + home + '#' + away,
        'year': CURRENT_YEAR,
        'data': 'COMPLETED#false',
        'home': home,
        'away': away,
        'home_score': 0,
        'away_score': 0,
    })