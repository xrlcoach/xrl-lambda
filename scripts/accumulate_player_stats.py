from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
import sys
import math

CURRENT_YEAR = 2022
round_number = 1

dynamodbResource = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodbResource.Table('XRL2021')
# all_stats = stats_table.scan()['Items']
squads = table.query(
  IndexName='sk-data-index',
  KeyConditionExpression=Key('sk').eq(
      'PROFILE') & Key('data').begins_with('TEAM')
)['Items']
positions_general = {
  'Fullback': 'Back',
  'Winger': 'Back',
  'Centre': 'Back',
  'Five-Eighth': 'Playmaker',
  'Halfback': 'Playmaker',
  'Hooker': 'Playmaker',
  'Prop': 'Forward',
  '2nd': 'Forward',
  '2nd Row': 'Forward',
  'Lock': 'Forward'
}
all_stats = []
for i in range(1, int(round_number)):
    all_stats += table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq(
            f'STATS#{CURRENT_YEAR}#' + str(i)) & Key('data').begins_with('CLUB#')
    )['Items']
appearances = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq(
        f'STATS#{CURRENT_YEAR}#' + str(round_number)) & Key('data').begins_with('CLUB#')
)['Items']
all_stats += appearances
for player in appearances:
    if player['stats']['Position'] in ['Interchange', 'Reserve', 'Replacement']:
        continue
    player_info = [p for p in squads if p['player_id']
                    == player['player_id']]
    if len(player_info) == 0:
        continue
    player_info = player_info[0]
    played_position = positions_general[player['stats']['Position']]
    if played_position not in [player_info['position'], player_info['position2']]:
        print(
            f"{player['player_name']} played as a {played_position} but is not recognised as such. Making a note on his player record.")
        if 'new_position_appearances' not in player_info.keys():
            player_info['new_position_appearances'] = {}
        if played_position not in player_info['new_position_appearances'].keys():
            player_info['new_position_appearances'][played_position] = 1
        else:
            player_info['new_position_appearances'][played_position] += 1
        table.update_item(
            Key={
                'pk': player_info['pk'],
                'sk': 'PROFILE'
            },
            UpdateExpression="set new_position_appearances=:npa",
            ExpressionAttributeValues={
                ':npa': player_info['new_position_appearances']
            }
        )
        if player_info['new_position_appearances'][played_position] == 3:
            print(
                f"{player['player_name']} has played as a {played_position} three times. Adding {played_position} to his positions.")
            if player_info['position2'] == None or player_info['position2'] == '':
                table.update_item(
                    Key={
                        'pk': player_info['pk'],
                        'sk': 'PROFILE'
                    },
                    UpdateExpression="set position2=:v",
                    ExpressionAttributeValues={
                        ':v': played_position
                    }
                )
            else:
                print(
                    f"{player['player_name']} can now play in all 3 positions!")
                table.update_item(
                    Key={
                        'pk': player_info['pk'],
                        'sk': 'PROFILE'
                    },
                    UpdateExpression="set position3=:v",
                    ExpressionAttributeValues={
                        ':v': played_position
                    }
                )
print('Positional updates complete. Updating player total stats')
for player in squads:
    player_stats = {}
    player_appearances = [
        stat for stat in all_stats if stat['player_id'] == player['player_id']]
    if len(player_appearances) == 0:
        continue
    player_stats['stats'] = {}
    player_stats['stats']['appearances'] = len(player_appearances)
    player_stats['scoring_stats'] = {}
    for app in player_appearances:
        for stat in app['stats'].keys():
            if type(app['stats'][stat]) is str or type(app['stats'][stat]) is dict:
                continue
            if app['stats'][stat] % 1 != 0:
                continue
            if stat not in player_stats['stats']:
                player_stats['stats'][stat] = 0
            player_stats['stats'][stat] += app['stats'][stat]
        for position in app['scoring_stats'].keys():
            if position not in player_stats['scoring_stats']:
                player_stats['scoring_stats'][position] = {}
            for stat in app['scoring_stats'][position].keys():
                if stat not in player_stats['scoring_stats'][position]:
                    player_stats['scoring_stats'][position][stat] = 0
                if stat == 'send_offs':
                    if 'send_off_deduction' not in player_stats['scoring_stats'][position]:
                        player_stats['scoring_stats'][position]['send_off_deduction'] = 0
                    if app['scoring_stats'][position][stat] != 0:
                        player_stats['scoring_stats'][position][stat] += 1
                        minutes = 80 - \
                            int(app['scoring_stats'][position][stat])
                        deduction = math.floor(minutes / 10) + 4
                        player_stats['scoring_stats'][position]['send_off_deduction'] += deduction
                else:
                    player_stats['scoring_stats'][position][stat] += app['scoring_stats'][position][stat]
    for position in player_stats['scoring_stats'].keys():
        if position == 'kicker':
            player_stats['scoring_stats'][position]['points'] = player_stats['scoring_stats'][position]['goals'] * 2
        else:
            player_stats['scoring_stats'][position]['points'] = player_stats['scoring_stats'][position]['tries'] * 4
            player_stats['scoring_stats'][position]['points'] += + player_stats['scoring_stats'][position]['field_goals'] + \
                player_stats['scoring_stats'][position]['2point_field_goals'] * 2
            player_stats['scoring_stats'][position]['points'] += player_stats['scoring_stats'][position]['involvement_try'] * 4
            player_stats['scoring_stats'][position]['points'] += player_stats['scoring_stats'][position]['positional_try'] * 4
            player_stats['scoring_stats'][position]['points'] -= player_stats['scoring_stats'][position]['mia'] * 4
            player_stats['scoring_stats'][position]['points'] -= player_stats['scoring_stats'][position]['concede'] * 4
            player_stats['scoring_stats'][position]['points'] -= player_stats['scoring_stats'][position]['sin_bins'] * 2
            player_stats['scoring_stats'][position]['points'] -= player_stats['scoring_stats'][position]['send_off_deduction']
    #print('Updating ' + player['player_name'])
    # table.put_item(
    #     Item={
    #         'pk': player['pk'],
    #         'sk': f'YEARSTATS#{CURRENT_YEAR}',
    #         'data': 'PLAYER_NAME#' + player['player_name'],
    #         'stats': player_stats['stats'],
    #         'scoring_stats': player_stats['scoring_stats'],
    #         'player_name': player['player_name'],
    #         'search_name': player['search_name'],
    #         'year': CURRENT_YEAR
    #     }
    # )
    table.update_item(
        Key={
            'pk': player['pk'],
            'sk': 'PROFILE'
        },
        UpdateExpression="set stats=:stats, scoring_stats=:scoring_stats",
        ExpressionAttributeValues={
            ':stats': player_stats['stats'],
            ':scoring_stats': player_stats['scoring_stats']
        }
    )

print("Script completed")
