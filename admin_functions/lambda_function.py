import boto3
import decimal
import json
from boto3.dynamodb.conditions import Key, Attr
from datetime import date, datetime, timedelta

CURRENT_YEAR = 2023

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')


class CreatePlayerRequest:
    def __init__(self, data):
        self.player_name = data['player_name'] if 'player_name' in data.keys(
        ) else None
        self.nrl_club = data['nrl_club'] if 'nrl_club' in data.keys(
        ) else None
        self.position = data['position'] if 'position' in data.keys(
        ) else None


class UpdatePlayerRequest:
    def __init__(self, data):
        self.player_id = data['player_id'] if 'player_id' in data.keys(
        ) else None
        self.player_name = data['player_name'] if 'player_name' in data.keys(
        ) else None
        self.nrl_club = data['nrl_club'] if 'nrl_club' in data.keys(
        ) else None
        self.position = data['position'] if 'position' in data.keys(
        ) else None
        self.position2 = data['position2'] if 'position2' in data.keys(
        ) else None
        self.position3 = data['position3'] if 'position3' in data.keys(
        ) else None


class DeletePlayerRequest:
    def __init__(self, data):
        self.player_id = data['player_id'] if 'player_id' in data.keys(
        ) else None


class AdminPostRequest:
    def __init__(self, data):
        self.operation = data['operation'] if 'operation' in data.keys(
        ) else None
        self.new_player = CreatePlayerRequest(data['new_player']) if 'new_player' in data.keys(
        ) else None
        self.update_player = UpdatePlayerRequest(data['update_player']) if 'update_player' in data.keys(
        ) else None
        self.delete_player = DeletePlayerRequest(data['delete_player']) if 'delete_player' in data.keys(
        ) else None


def lambda_handler(event, context):
    # Find request method
    method = event["httpMethod"]
    if method == 'GET':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            'body': json.dumps("GET is working on this route")
        }
    if method == 'POST':
        try:
            # POST request should contain an 'operation' property in the request body
            print('Method is POST, checking operation')
            body = AdminPostRequest(json.loads(event['body']))
            operation = body.operation
            print("Operation is " + operation)
            if operation == 'create_player':
                new_player = body.new_player
                if not new_player or not new_player.player_name or not new_player.nrl_club or not new_player.position:
                    raise Exception("Insufficient new player data provided")
                print("Fetching squads")
                squads = table.query(
                    IndexName='sk-data-index',
                    KeyConditionExpression=Key('sk').eq(
                        'PROFILE') & Key('data').begins_with('TEAM')
                )['Items']
                player_id = str(max([int(p['player_id']) for p in squads]) + 1)
                print(f"New player id will be {player_id}")
                player = {
                    'pk': 'PLAYER#' + player_id,
                    'sk': 'PROFILE',
                    'data': 'TEAM#None',
                    'player_id': player_id,
                    'player_name': new_player.player_name,
                    'nrl_club': new_player.nrl_club,
                    'xrl_team': 'None',
                    'search_name': new_player.player_name.lower(),
                    'position': new_player.position,
                    'position2': None,
                    'position3': None,
                    'stats': {},
                    'scoring_stats': {
                        new_player.position: {},
                        'kicker': {}
                    },
                    'times_as_captain': 0,
                    'new_position_appearances': {}
                }
                table.put_item(
                    Item=player
                )
                print("Player added. Returning data")
                return {
                    'statusCode': 200,
                    'headers': {
                        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(player))
                }
            if operation == 'delete_player':
                if not body.delete_player.player_id:
                    raise Exception("Player id not provided")
                print(f"Deleting player with id {body.delete_player.player_id}")
                table.delete_item(Key={
                    'pk': 'PLAYER#' + body.delete_player.player_id,
                    'sk': 'PROFILE'
                })
                print("Player deleted. Returning success response")
                return {
                    'statusCode': 200,
                    'headers': {
                        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps("Player deleted")
                }
            if operation == 'update_player':
                if not body.update_player.player_id:
                    raise Exception("Player id not provided")
                player = table.get_item(Key={
                    'pk': 'PLAYER#' + body.update_player.player_id,
                    'sk': 'PROFILE'
                })['Item']
                if not player:
                    raise Exception('Player not found')
                print(f"Updating {player['player_name']}")
                scoring_stats = player['scoring_stats']
                if body.update_player.position2 and not player['position2']:
                    scoring_stats[body.update_player.position2] = {}
                if body.update_player.position3 and not player['position3']:
                    scoring_stats[body.update_player.position3] = {}
                table.update_item(
                    Key={
                        'pk': player['pk'],
                        'sk': 'PROFILE'
                    },
                    UpdateExpression="set player_name = :name, nrl_club = :club, #P = :pos, position2 = :pos2, position3 = :pos3, scoring_stats = :ss",
                    ExpressionAttributeNames={
                        '#P': 'position'
                    },
                    ExpressionAttributeValues={
                        ':name': body.update_player.player_name,
                        ':club': body.update_player.nrl_club,
                        ':pos': body.update_player.position,
                        ':pos2': body.update_player.position2,
                        ':pos3': body.update_player.position3,
                        ':ss': scoring_stats
                    }
                )
                print(f"Player updated. Fetching updated record")
                updated_player = table.get_item(
                    Key={'pk': player['pk'], 'sk': 'PROFILE' })['Item']
                print("Returning updated player")
                return {
                    'statusCode': 200,
                    'headers': {
                        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(updated_player))
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
