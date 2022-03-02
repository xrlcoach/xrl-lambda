import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import date, datetime, timedelta
import json
import decimal
import sys

CURRENT_YEAR = 2022

def lambda_handler(event, context):
    print(f"Script executing at {(datetime.now() + timedelta(hours=11)).strftime('%c')}")
    report = f"Script executing at {(datetime.now() + timedelta(hours=11)).strftime('%c')}"

    dynamodbResource = boto3.resource('dynamodb', 'ap-southeast-2')
    table = dynamodbResource.Table('XRL2021')

    #Find current active round
    resp = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('STATUS') & Key('data').eq('ACTIVE#true'),
        FilterExpression=Attr('year').eq(CURRENT_YEAR)
    )
    round_number = max([r['round_number'] for r in resp['Items']])
    print(f"Current XRL round: {round_number}. Second round of waivers. Multiple waivers allowed.")
    report += f"\nCurrent XRL round: {round_number}. Second round of waivers. Multiple waivers allowed."

    #Get all players
    all_players = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM#')
    )['Items']

    #Get all xrl teams
    users = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').begins_with('NAME#')
    )['Items']

    #Sort users by waiver rank
    waiver_order = sorted(users, key=lambda u: u['waiver_rank'])
    print("Current waiver order:")
    report += "\nCurrent waiver order:"
    for rank, user in enumerate(waiver_order, 1):
        print(f"{rank}. {user['team_name']}")
        report += f"\n{rank}. {user['team_name']}"
        table.put_item(
            Item={
                'pk': f'PREFS#{CURRENT_YEAR}#{round_number}#Wednesday',
                'sk': user['pk'],
                'data': 'WAIVERS',
                'round_number': str(round_number),
                'year': CURRENT_YEAR,
                'day': 'Wednesday',
                'preferences': user['waiver_preferences']
            }
        )

    players_transferred = []

    print("Processing waivers")
    #Iterate through users
    while True:
        players_transferred_in_round = 0
        users_who_picked = []
        for rank, user in enumerate(waiver_order, 1):
            print(f"User {rank} - {user['team_name']}")
            report += f"\nUser {rank} - {user['team_name']}"

            users_squad = [player for player in all_players if player['xrl_team'] == user['team_short']]
            preferences = user['waiver_preferences']
            gained_player = False
            #If user has no preferences left, continue
            if len(preferences) == 0:
                print(f"{user['team_name']} has no remaining preferences.")
                report += f"\n{user['team_name']} has no remaining preferences."
                continue
            #Iterate through user's waiver preferences
            while len(preferences) > 0:
                preference = preferences[0]
                player_id = preference['pick']
                player_info = next((p for p in all_players if p['player_id'] == player_id), None)
                pickable = False
                print(f"{user['team_name']} want to sign {player_info['player_name']}.")
                report += f"\n{user['team_name']} want to sign {player_info['player_name']}."
                #Check if player not already picked and available to be picked
                if player_id not in players_transferred and ('xrl_team' not in player_info.keys() or player_info['xrl_team'] == 'None' or player_info['xrl_team'] == 'On Waivers' or player_info['xrl_team'] == 'Pre-Waivers'):
                    print(f"{player_info['player_name']} is available.")
                    report += f"\n{player_info['player_name']} is available."
                    #Check if user already has 18 players in squad
                    if len(users_squad) == 18:
                        #Check drop preference list
                        drop_player_id = None
                        drop_player_record = None
                        drop_preferences = preference['drop']
                        for drop_id in drop_preferences:
                            if drop_id in players_transferred:
                                continue
                            drop_player_record = next((p for p in all_players if p['player_id'] == drop_id), None)
                            if drop_player_record == None or drop_player_record['xrl_team'] != user['team_short']:
                                continue
                            drop_player_id = drop_id
                            break
                        #If no eligible drop preference found, continue to next user
                        if drop_player_id == None:
                            print(f"{user['team_name']}'s squad already has 18 players and they have no eligible player to drop. Moving to next user.")
                            report += f"\n{user['team_name']}'s squad already has 18 players and they have no eligible player to drop. Moving to next user."
                            break
                        #Else drop preferred player and list desired player as 'pickable'
                        else:
                            print(f"{user['team_name']}'s squad has 18 players. Dropping {drop_player_record['player_name']} to make room.")
                            report += f"\n{user['team_name']}'s squad has 18 players. Dropping {drop_player_record['player_name']} to make room."
                            #Add their provisional drop player to the array of players transferred
                            players_transferred.append(drop_player_id)
                            #Remove player from user's next lineup
                            table.delete_item(
                                Key={
                                    'pk': 'PLAYER#' + drop_player_id,
                                    'sk': f'LINEUP#{CURRENT_YEAR}#' + str(round_number)
                                }
                            )
                            #Update player's XRL team from the user to Pre-Waivers (This means they will remain
                            #on waiver for one whole round)
                            drop_player_record['xrl_team'] = 'Pre-Waivers'
                            drop_player_record['data'] = 'TEAM#Pre-Waivers'
                            table.update_item(
                                Key={
                                    'pk': 'PLAYER#' + drop_player_id,
                                    'sk': 'PROFILE'
                                },
                                UpdateExpression="set #D=:d, xrl_team=:t",
                                ExpressionAttributeNames={
                                    '#D': 'data'
                                },
                                ExpressionAttributeValues={
                                    ':d': 'TEAM#Pre-Waivers',
                                    ':t': 'Pre-Waivers'
                                }
                            )
                            #Add record of drop to transfers table
                            transfer_date = datetime.now() + timedelta(hours=11)
                            table.put_item(
                                Item={
                                    'pk': 'TRANSFER#' + user['username'] + str(transfer_date),
                                    'sk': 'TRANSFER',
                                    'data': f'ROUND#{CURRENT_YEAR}#' + str(round_number),
                                    'user': user['username'],                        
                                    'datetime': transfer_date.strftime("%c"),
                                    'type': 'Drop',
                                    'round_number': round_number,
                                    'year': CURRENT_YEAR,
                                    'player_id': drop_player_id
                                }
                            )
                            #Set boolean saying user may sign new player
                            pickable = True
                    else:
                        #If player is available AND user's squad has less than 18 players,
                        #then set boolean saying new player is pickable
                        pickable = True
                else:
                    #If player has already been transferred in this session, or their XRL team is not 'None',
                    #'Pre-Waivers' or 'On Waivers', then they are not available to pick
                    print(f"{player_info['player_name']} is not available.")
                    report += f"\n{player_info['player_name']} is not available."
                    #Remove player from waiver preferences
                    preferences.pop(0)

                if pickable:
                    #If player can be signed, update their XRL team to the user's team acronym
                    player_info['xrl_team'] = user['team_short']
                    player_info['data'] = 'TEAM#' + user['team_short']
                    table.update_item(
                        Key={
                            'pk': 'PLAYER#' + player_id,
                            'sk': 'PROFILE'
                        },
                        UpdateExpression="set #D=:d, xrl_team=:t",
                        ExpressionAttributeNames={
                            '#D': 'data'
                        },
                        ExpressionAttributeValues={
                            ':d': 'TEAM#' + user['team_short'],
                            ':t': user['team_short']
                        }
                    )
                    #Add a record of the transfer to the db
                    transfer_date = datetime.now() + timedelta(hours=11)
                    table.put_item(
                        Item={
                            'pk': 'TRANSFER#' + user['username'] + str(transfer_date),
                            'sk': 'TRANSFER',
                            'data': f'ROUND#{CURRENT_YEAR}#' + str(round_number),
                            'user': user['username'],                        
                            'datetime': transfer_date.strftime("%c"),
                            'type': 'Waiver',
                            'round_number': round_number,
                            'year': CURRENT_YEAR,
                            'player_id': player_id
                        }
                    )
                    #Add a message to the user's inbox
                    message = {
                        "sender": "XRL Admin",
                        "datetime": (datetime.now() + timedelta(hours=11)).strftime("%c"),
                        "subject": "New Player",
                        "message": f"Congratulations! You picked up {player_info['player_name']} in this week's waivers."
                    }
                    user['inbox'].append(message)
                    #Indicate that the user has signed a player
                    gained_player = True
                    #Add player to list of players transferred
                    players_transferred.append(player_id)
                    print(f"{user['team_name']} signed {player_info['player_name']}")
                    report += f"\n{user['team_name']} signed {player_info['player_name']}"
                    #Remove player from waiver preferences
                    preferences.pop(0)
                    break
            
            #Indicate whether the curent user has picked a player or not
            if gained_player:
                user['players_picked'] += 1
                users_who_picked.append(user)
                players_transferred_in_round += 1
            else:
                print(f"{user['team_name']} didn't get any of their remaining preferences")
                report += f"\n{user['team_name']} didn't get any of their remaining preferences"            
            #Update user's waiver preferences, players_picked attribute and inbox
            table.update_item(
                Key={
                    'pk': user['pk'],
                    'sk': 'DETAILS'
                },
                UpdateExpression="set waiver_preferences=:wp, players_picked=players_picked+:v, inbox=:i",
                ExpressionAttributeValues={
                    ':wp': [],
                    ':v': user['players_picked'],
                    ':i': user['inbox']
                }
            )
        #Recalculate waiver order (players who didn't pick followed by those who did in reverse order)
        waiver_order = [u for u in waiver_order if u not in users_who_picked] + users_who_picked[::-1]
        if players_transferred_in_round == 0:
            break

    #Save new waiver order to db 
    print("New waiver order:")
    report += "\nNew waiver order:"
    for rank, user in enumerate(waiver_order, 1):
        print(f"{rank}. {user['team_name']}")
        report += f"\n{rank}. {user['team_name']}"
        table.update_item(
            Key={
                'pk': user['pk'],
                'sk': 'DETAILS'
            },
            UpdateExpression="set waiver_rank=:wr",
            ExpressionAttributeValues={
                ':wr': rank
            }
        )

    #Add waiver report to db
    table.put_item(
        Item={
            'pk': 'WAIVER',
            'sk': f'REPORT#{CURRENT_YEAR}#' + str(round_number) + '_Wednesday',
            'data': 'WAIVER_REPORT',
            'waiver_round': str(round_number) + '_Wednesday',
            'report': report,
            'year': CURRENT_YEAR
        }
    )


    
