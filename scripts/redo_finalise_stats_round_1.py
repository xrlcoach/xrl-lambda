import math
import sys
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Attr, Key

CURRENT_YEAR = 2025

def finalise_stats():

    print(f"Script executing at {datetime.now()}")

    dynamodbResource = boto3.resource('dynamodb', 'ap-southeast-2')
    table = dynamodbResource.Table('XRL2021')

    round_number = 1
    print(f"Finalising Round {round_number}")
    fixtures = table.query(
        KeyConditionExpression=Key('pk').eq(f'ROUND#{CURRENT_YEAR}#' + str(round_number)) & Key('sk').begins_with('FIXTURE')
    )['Items']

    lineups = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq(f'LINEUP#{CURRENT_YEAR}#' + str(round_number)) & Key('data').begins_with('TEAM#')
    )['Items']
    #print(str(lineups[0]))
    users = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').begins_with('NAME#')
    )['Items']
    #print(str(users[0]))

    print("Finalising lineup substitutions and scores...")
    for match in fixtures:
        print(f"Finalising {match['home']} v {match['away']}")
        for team in ['home', 'away']:
            print(f"Finalising {match[team]} lineup")
            user = [u for u in users if u['team_short'] == match[team]][0]
            lineup = [player for player in lineups if player['xrl_team'] == match[team]]
            
            starters = [player for player in lineup if player['position_number'] < 14]            
            bench = [player for player in lineup if player['position_number'] >= 14]
            print("Making substitutions")
            #Calculate free spots (i.e. number of starters who didn't play NRL) for subs
            freeSpots = {
                'Back': len([p for p in starters if p['position_general'] == 'Back' and not p['played_nrl']]),
                'Playmaker': len([p for p in starters if p['position_general'] == 'Playmaker' and not p['played_nrl']]),
                'Forward': len([p for p in starters if p['position_general'] == 'Forward' and not p['played_nrl']])
            }
            #Sub in any bench players who are the right position for free spots
            for sub in sorted(bench, key=lambda p: p['position_number']):
                subbed_in = False
                if freeSpots[sub['position_general']] > 0:
                    if sub['played_nrl']:
                        print(f"Subbing in {sub['player_name']} as a {sub['position_general']}")
                        freeSpots[sub['position_general']] -= 1
                        subbed_in = True
                        table.update_item(
                            Key={
                                'pk': sub['pk'],
                                'sk': sub['sk']
                            },
                            UpdateExpression="set played_xrl=:p",
                            ExpressionAttributeValues={
                                ':p': True
                            }
                        )
                if not subbed_in and sub['second_position'] != '' and sub['second_position'] != None:
                    if freeSpots[sub['second_position']] > 0:
                        print(f"Subbing in {sub['player_name']} as a {sub['second_position']}")
                        freeSpots[sub['second_position']] -= 1
                        subbed_in = True
                        table.update_item(
                            Key={
                                'pk': sub['pk'],
                                'sk': sub['sk']
                            },
                            UpdateExpression="set played_xrl=:p",
                            ExpressionAttributeValues={
                                ':p': True
                            }
                        )

            #Get final lineup
            final_lineup = [player for player in lineup if player['played_xrl']]
            print("Substitutions complete.")

            vice_plays = False
            backup_kicks = False
            print("Checking if captain(s) and kicker played")
            for player in lineup:
                if not player['played_nrl']:
                    if player['captain'] or player['captain2']:
                        print(f"Captain {player['player_name']} did not play.")
                        table.update_item(
                            Key={
                                'pk': 'PLAYER#' + player['player_id'],
                                'sk': 'PROFILE'
                            },
                            UpdateExpression="set times_as_captain = :i",
                            ExpressionAttributeValues={
                                ':i': 0
                            }
                        )
                        vice_plays = True
                    if player['kicker']:
                        print(f"Kicker {player['player_name']} did not play.")
                        backup_kicks = True
            for player in final_lineup:
                if player['played_nrl']:          
                    if player['vice'] and vice_plays:
                        print(f"{player['player_name']} takes over captaincy duties. Adjusting lineup score and user's captain counts.")
                        if player['kicker']:
                            final_score = player['playing_score'] * 2 + player['kicking_score']
                        else:
                            final_score = player['playing_score'] * 2
                        table.update_item(
                            Key={
                                'pk': player['pk'],
                                'sk': player['sk']
                            },
                            UpdateExpression="set score=:v",
                            ExpressionAttributeValues={
                                ':v': final_score
                            }                        
                        )                    
                        table.update_item(
                            Key={
                                'pk': player['pk'],
                                'sk': 'PROFILE'
                            },
                            UpdateExpression="set times_as_captain = :i",
                            ExpressionAttributeValues={
                                ':i': 1
                            }
                        )
                    if player['backup_kicker'] and backup_kicks:
                        print(f"{player['player_name']} takes over kicking duties. Adjusting score.")
                        table.update_item(
                            Key={
                                'pk': player['pk'],
                                'sk': player['sk']
                            },
                            UpdateExpression="set score=score+:s",
                            ExpressionAttributeValues={
                                ':s': player['kicking_score']
                            }
                        )
                    

    print("Captain and kicker assignments done. Finalising match results...")
    lineups = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq(f'LINEUP#{CURRENT_YEAR}#' + str(round_number)) & Key('data').begins_with('TEAM#')
    )['Items']

    for match in fixtures:
        home_user = [user for user in users if user['team_short'] == match['home']][0]
        home_lineup = [player for player in lineups if player['xrl_team'] == match['home']]
        home_score = sum([p['score'] for p in home_lineup if p['played_xrl']])
        match['home_score'] = home_score
        away_user = [user for user in users if user['team_short'] == match['away']][0]
        away_lineup = [player for player in lineups if player['xrl_team'] == match['away']]
        away_score = sum([p['score'] for p in away_lineup if p['played_xrl']])
        match['away_score'] = away_score

        home_user['stats']['for'] = home_score
        home_user['stats']['against'] = away_score
        away_user['stats']['for'] = away_score
        away_user['stats']['against'] = home_score
        table.update_item(
            Key={
                'pk': 'USER#' + home_user['username'],
                'sk': 'DETAILS'
            },
            UpdateExpression="set stats=:s",
            ExpressionAttributeValues={
                ':s': home_user['stats']
            }
        )
        table.update_item(
            Key={
                'pk': 'USER#' + away_user['username'],
                'sk': 'DETAILS'
            },
            UpdateExpression="set stats=:s",
            ExpressionAttributeValues={
                ':s': away_user['stats']
            }
        )
        table.update_item(
            Key={
                'pk': match['pk'],
                'sk': match['sk']
            },
            UpdateExpression='set home_score=:hs, away_score=:as, #D=:d',
            ExpressionAttributeNames={
                '#D': 'data'
            },
            ExpressionAttributeValues={
                ':hs': match['home_score'],
                ':as': match['away_score'],
                ':d': 'COMPLETED#true'
            }
        )
        

    # all_stats = stats_table.scan()['Items']
    squads = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
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
    appearances = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq(f'STATS#{CURRENT_YEAR}#' + str(round_number)) & Key('data').begins_with('CLUB#')
    )['Items']
    all_stats += appearances
    print('Positional updates complete. Updating player total stats')
    for player in squads:
        player_stats = {}
        player_appearances = [stat for stat in all_stats if stat['player_id'] == player['player_id']]
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
                            minutes = 80 - int(app['scoring_stats'][position][stat])
                            deduction = math.floor(minutes / 10) + 4
                            player_stats['scoring_stats'][position]['send_off_deduction'] += deduction
                    else:
                        player_stats['scoring_stats'][position][stat] += app['scoring_stats'][position][stat]
        for position in player_stats['scoring_stats'].keys():
            if position == 'kicker':
                player_stats['scoring_stats'][position]['points'] = player_stats['scoring_stats'][position]['goals'] * 2
            else:
                player_stats['scoring_stats'][position]['points'] = player_stats['scoring_stats'][position]['tries'] * 4
                player_stats['scoring_stats'][position]['points'] += + player_stats['scoring_stats'][position]['field_goals'] + player_stats['scoring_stats'][position]['2point_field_goals'] * 2
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
    
if __name__ == '__main__':
    finalise_stats()