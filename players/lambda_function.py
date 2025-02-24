import base64
import decimal
import hashlib
import json
from datetime import date, datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Attr, Key

CURRENT_YEAR = 2025

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

class PlayersGetRequest: 
    def __init__(self, params):
        self.nrlClub = params['nrlClub'] if 'nrlClub' in params.keys() else None
        self.xrlTeam = params['xrlTeam'] if 'xrlTeam' in params.keys() else None
        self.playerId = params['playerId'] if 'playerId' in params.keys() else None
        self.news = params['news'] if 'news' in params.keys() else None
        self.year = int(params['year']) if 'year' in params.keys() else None


class PlayersPostRequest:
    def __init__(self, data):
        self.operation = data['operation'] if 'operation' in data.keys() else None
        self.players = data['players'] if 'players' in data.keys() else None
        self.xrl_team = data['xrl_team'] if 'xrl_team' in data.keys() else None


def lambda_handler(event, context):
    #Find request method
    method = event["httpMethod"]
    if method == 'GET':
        try:
            print('Method is get, checking for params')
            #If there is no query added to fetch GET request, scan the whole players table
            if not event["queryStringParameters"]:
                print('No params found, scanning table')
                start = datetime.now() + timedelta(hours=11)
                # resp = table.scan()['Items']
                players = table.query(
                    IndexName='sk-data-index',
                    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
                )['Items']
                # for player in players:
                #     yearstats = table.get_item(Key={
                #         'pk': player['pk'],
                #         'sk': f'YEARSTATS#{CURRENT_YEAR}'
                #     })
                #     if 'Item' not in yearstats.keys():
                #         player['stats'] = {}
                #         player['scoring_stats'] = {}
                #     else:
                #         player['stats'] = yearstats['Item']['stats']
                #         player['scoring_stats'] = yearstats['Item']['scoring_stats']
                finish = datetime.now() + timedelta(hours=11)
                print(f'Table scan copmlete in {finish - start}. Returning json response')
            else:
                #If query string attached to GET request, determine request parameters and query players table accordingly
                print('Params detected')        
                params = PlayersGetRequest(event["queryStringParameters"])
                print(event["queryStringParameters"])
                print(params.news)
                if params.year:
                    players = table.query(
                        IndexName='sk-data-index',
                        KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
                    )['Items']
                    if params.year != CURRENT_YEAR:
                        yearstats = table.query(
                            IndexName='sk-data-index',
                            KeyConditionExpression=Key('sk').eq(f'YEARSTATS#{params.year}') & Key('data').begins_with('PLAYER_NAME')
                        )['Items']
                        for player in players:
                            player['year'] = params.year
                            player_stats = next((stats for stats in yearstats if stats['pk'] == player['pk']), None)
                            if player_stats == None:
                                player['stats'] = {}
                                player['scoring_stats'] = { player['position']: {}, 'kicker': {}}
                            else:
                                player['stats'] = player_stats['stats']
                                player['scoring_stats'] = player_stats['scoring_stats']
                                positions = player['scoring_stats'].keys()
                                positions = [pos for pos in positions if pos != 'kicker']
                                if len(positions) > 0 and player['position'] not in positions:
                                    player['position'] = positions[0]
                elif params.nrlClub:
                    nrlClub = params.nrlClub
                    print(f'NrlClub param is {nrlClub}, querying table')
                    # resp = table.scan(
                    #     FilterExpression=Attr('nrl_club').eq(nrlClub)
                    # )['Items']
                    players = table.query(
                        IndexName='sk-data-index',
                        KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM'),
                        FilterExpression=Attr('nrl_club').eq(nrlClub)
                    )['Items']
                elif params.xrlTeam:
                    xrlTeam = params.xrlTeam
                    print(f'XrlTeam param is {xrlTeam}, querying table')
                    if xrlTeam == 'Free Agents':
                        # resp = table.scan(
                        #     FilterExpression=Attr('xrl_team').not_exists() | Attr('xrl_team').eq('None') | Attr('xrl_team').eq('On Waivers') | Attr('xrl_team').eq('Pre-Waivers')
                        # )['Items']
                        players = table.query(
                            IndexName='sk-data-index',
                            KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM#'),
                            FilterExpression=Attr('xrl_team').eq('None') | Attr('xrl_team').eq('On Waivers') | Attr('xrl_team').eq('Pre-Waivers')
                        )['Items']
                    else:
                        # resp = table.scan(
                        #     FilterExpression=Attr('xrl_team').eq(xrlTeam)
                        # )['Items']
                        players = table.query(
                            IndexName='sk-data-index',
                            KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').eq('TEAM#' + xrlTeam)
                        )['Items']
                elif params.playerId:
                    player_id = params.playerId
                    print(f'PlayerId param is {player_id}, querying table')
                    # resp = table.get_item(
                    #     Key={
                    #         'player_id': player_id
                    #     }
                    # )['Item']
                    players = table.get_item(Key={
                        'pk': 'PLAYER#' + player_id,
                        'sk': 'PROFILE'
                    })['Item']
                elif params.news:
                    round_no = params.news
                    print(f'Request for player news from round {round_no}, querying table')
                    players = table.query(
                        KeyConditionExpression=Key('pk').eq('NEWS') & Key('sk').begins_with('PLAYER'),
                        FilterExpression=Attr('data').eq(f'ROUND#{CURRENT_YEAR}#{round_no}')
                    )['Items']
                #If query parameters present but are not any of the above, send back error message
                else:
                    print("Couldn't recognise parameter")
                    resp = {"error": "GET request parameter not recognised"}
            print('Returning respnse')
            #Return response
            return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(players))
                }
        except Exception as e:
                print(e)
                return {
                    'statusCode': 500,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps({"error": str(e)})
                }
    if method == 'POST':
        try:
            #POST request should contain an 'operation' property in the request body
            print('Method is POST, checking operation')
            body = PlayersPostRequest(json.loads(event['body']))
            print("Operation is " + body.operation)
            # users = users_table.scan()['Items']
            # active_user = [u for u in users if u['team_short'] == body['xrl_team']][0]
            if body.operation == 'get_players':
                player_ids = body.players
                players = []
                for player_id in player_ids:
                    resp = table.get_item(Key={
                        'pk': 'PLAYER#' + player_id,
                        'sk': 'PROFILE'
                    })
                    if 'Item' in resp.keys():
                        players.append(resp['Item'])
                return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(players))
                }
            active_user = table.query(
                IndexName='sk-data-index',
                KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').eq('NAME#' + body.xrl_team)
            )['Items'][0]
            print(f"Active user is {active_user['username']}")
            # rounds = rounds_table.scan(
            #     FilterExpression=Attr('active').eq(True)
            # )['Items']
            rounds = table.query(
                IndexName='sk-data-index',
                KeyConditionExpression=Key('sk').eq('STATUS') & Key('data').eq('ACTIVE#true'),
                FilterExpression=Attr('year').eq(CURRENT_YEAR)
            )['Items']
            round_number = max([r['round_number'] for r in rounds])
            active_round = [r for r in rounds if r['round_number'] == round_number][0]
            print(f"Current round: {round_number}.")
            if body.operation == "scoop":
                if not active_round['scooping']:
                    raise Exception("Scooping is not permitted at this time")
                #Iterate through all players being scooped
                for player in body.players:
                    #Check if player is available to be scooped
                    # player_record = table.get_item(
                    #     Key={
                    #         'player_id': player['player_id']
                    #     }
                    # )['Item']
                    player_record = table.get_item(
                        Key={
                            'pk': 'PLAYER#' + player['player_id'],
                            'sk': 'PROFILE'
                        }
                    )['Item']
                    if 'xrl_team' in player_record.keys() and player_record['xrl_team'] != 'None':
                        raise Exception(f"{player['player_name']} has already signed for another XRL team.")
                for player in body.players:
                    #Update player's XRL team
                    # table.update_item(
                    #     Key={
                    #         'player_id': player['player_id'],
                    #     },
                    #     UpdateExpression="set xrl_team=:x",
                    #     ExpressionAttributeValues={
                    #         ':x': body['xrl_team']
                    #     }
                    # )
                    table.update_item(
                        Key={
                            'pk': 'PLAYER#' + player['player_id'],
                            'sk': 'PROFILE'
                        },
                        UpdateExpression="set #D=:d, xrl_team=:x",
                        ExpressionAttributeNames={
                            '#D': 'data'
                        },
                        ExpressionAttributeValues={
                            ':d': 'TEAM#' + body.xrl_team,
                            ':x': body.xrl_team
                        }
                    )
                    # transfers_table.put_item(
                    #     Item={
                    #         'transfer_id': active_user['username'] + '_' + str(datetime.now()),
                    #         'user': active_user['username'],                        
                    #         'datetime': datetime.now().strftime("%c"),
                    #         'type': 'Scoop',
                    #         'round_number': round_number,
                    #         'player_id': player['player_id']
                    #     }
                    # ) 
                    if round_number > 1:
                        transfer_date = datetime.now() + timedelta(hours=10)
                        table.put_item(
                            Item={
                                'pk': 'TRANSFER#' + active_user['username'] + str(transfer_date),
                                'sk': 'TRANSFER',
                                'data': f'ROUND#{CURRENT_YEAR}#' + str(round_number),
                                'user': active_user['username'],                        
                                'datetime': transfer_date.strftime("%c"),
                                'type': 'Scoop',
                                'round_number': round_number,
                                'player_id': player['player_id'],
                                'year': CURRENT_YEAR
                            }
                        ) 
                    print(f"{player['player_name']}'s' XRL team changed to {body.xrl_team}") 
                if round_number > 1:               
                    print('Adjusting waiver order')
                    #Sort users by waiver rank
                    users = table.query(
                        IndexName='sk-data-index',
                        KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').begins_with('NAME#')
                    )['Items']
                    waiver_order = sorted(users, key=lambda u: u['waiver_rank'])
                    #Remove the user who just scooped a player and put them at the bottom of the list
                    waiver_order.remove(active_user)
                    waiver_order.append(active_user)
                    #Update everyone's waiver rank to reflect change
                    for rank, user in enumerate(waiver_order, 1):
                        table.update_item(
                            Key={
                                'pk': 'USER#' + user['username'],
                                'sk': 'DETAILS'
                            },
                            UpdateExpression="set waiver_rank=:wr",
                            ExpressionAttributeValues={
                                ':wr': rank
                            }
                        )
                    #Add the number of player's scooped to the user's 'players_picked' property 
                    print(f"Adding {len(body.players)} to {active_user['username']}'s picked players count")
                    table.update_item(
                        Key={
                            'pk': 'USER#' + user['username'],
                            'sk': 'DETAILS'
                        },
                        UpdateExpression="set players_picked=players_picked+:v",
                        ExpressionAttributeValues={
                            ':v': len(body.players)
                        }
                    )
                    print("Count updated")                   
            if body.operation == 'drop':
                #Iterate through players to be dropped
                # not_in_progress_rounds = table.query(
                #     IndexName='sk-data-index',
                #     KeyConditionExpression=Key('sk').eq('STATUS') & Key('data').begins_with('ACTIVE'),
                #     FilterExpression=Attr('in_progress').eq(False)
                # )['Items']
                next_round_number = round_number if not active_round['in_progress'] else round_number + 1
                for player in body.players:
                    # player_to_drop = table.get_item(
                    #     Key={
                    #         'player_id': player['player_id']
                    #     }
                    # )['Item']
                    # lineups_table.delete_item(
                    #     Key={
                    #         'name+nrl+xrl+round': player_to_drop['player_name'] + ';' + player_to_drop['nrl_club'] + ';' + active_user['team_short'] + ';' + str(next_round_number)
                    #     }
                    # )

                    #Remove them from any lineup for next round
                    table.delete_item(Key={
                        'pk': 'PLAYER#' + player['player_id'],
                        'sk': f'LINEUP#{CURRENT_YEAR}' + str(next_round_number)
                    })
                    #Check if they are user's provisional drop, and remove if they are
                    if player['player_id'] == active_user['provisional_drop']:
                        table.update_item(
                            Key={
                                'pk': active_user['pk'],
                                'sk': active_user['sk']
                            },
                            UpdateExpression="set provisional_drop=:pd",
                            ExpressionAttributeValues={
                                ':pd': None
                            }
                        )                
                    #Update their XRL team property to 'On Waivers'. This prevents them from being scooped until
                    #they clear the next round of waivers
                    new_team = 'None' if round_number == 1 else 'On Waivers'
                    table.update_item(
                        Key={
                            'pk': 'PLAYER#' + player['player_id'],
                            'sk': 'PROFILE'
                        },
                        UpdateExpression="set #D=:d, xrl_team=:x",
                        ExpressionAttributeNames={
                            '#D': 'data'
                        },
                        ExpressionAttributeValues={
                            ':d': 'TEAM#' + new_team,
                            ':x': new_team
                        }
                    )
                    #Add record to transfers table
                    transfer_date = datetime.now() + timedelta(hours=10)
                    table.put_item(
                        Item={
                            'pk': 'TRANSFER#' + active_user['username'] + str(transfer_date),
                            'sk': 'TRANSFER',
                            'data': f'ROUND#{CURRENT_YEAR}#' + str(round_number),
                            'user': active_user['username'],                        
                            'datetime': transfer_date.strftime("%c"),
                            'type': 'Drop',
                            'round_number': round_number,
                            'player_id': player['player_id'],
                            'year': CURRENT_YEAR
                        }
                    )
                    print(f"{player['player_name']} put on waivers")
            return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps({"message": "Player team updates successful"})
                }
        except Exception as e:
                print(e)
                return {
                    'statusCode': 500,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps({"error": str(e)})
                }
              

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