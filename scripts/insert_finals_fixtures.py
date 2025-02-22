import boto3
from boto3.dynamodb.conditions import Attr, Key

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

CURRENT_YEAR = 2025

users = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').begins_with('NAME#')
)['Items']

ladder = sorted(users, key = lambda user: (user['stats']['points'], user['stats']['for'] - user['stats']['against'], user['stats']['for']), reverse=True)

# print([u['team_name'] for u in users])

fixtures = [
    [ladder[0]['team_short'],ladder[3]['team_short']],
    [ladder[1]['team_short'],ladder[2]['team_short']],
    [ladder[4]['team_short'],ladder[7]['team_short']],
    [ladder[5]['team_short'],ladder[6]['team_short']],
    [ladder[8]['team_short'],ladder[11]['team_short']],
    [ladder[9]['team_short'],ladder[10]['team_short']],
]

print(fixtures)

for fixture in fixtures:
    home = fixture[0]
    away = fixture[1]
    table.put_item(Item={
        'pk': f'ROUND#{CURRENT_YEAR}#23',
        'sk': 'FIXTURE#' + home + '#' + away,
        'year': CURRENT_YEAR,
        'data': 'COMPLETED#false',
        'home': home,
        'away': away,
        'home_score': 0,
        'away_score': 0,
    })