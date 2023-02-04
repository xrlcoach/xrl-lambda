import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

squads = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
)['Items']

player_id = str(max([int(p['player_id']) for p in squads]) + 1)
player_name = 'Izaac Thompson'
position = 'Back'
club = 'Rabbitohs'
table.put_item(
    Item={
        'pk': 'PLAYER#' + player_id,
        'sk': 'PROFILE',
        'data': 'TEAM#None',
        'player_id': player_id,
        'player_name': player_name,
        'nrl_club': club,
        'xrl_team': 'None',
        'search_name': player_name.lower(),
        'position': position,
        'position2': None,
        'stats': {},
        'scoring_stats': {
            position: {},
            'kicker': {}
        },
        'times_as_captain': 0,
        'new_position_appearances': {}
    }                    
)