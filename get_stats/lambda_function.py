import decimal
import json

import boto3
from boto3.dynamodb.conditions import Attr, Key

CURRENT_YEAR = 2025

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
# table = dynamodb.Table('stats2020')
table = dynamodb.Table('XRL2021')

def lambda_handler(event, context):
    try:
        method = event['httpMethod']
        if method == 'GET':
            print('Method is get, checking for params')
            if not event["queryStringParameters"]:
                # print('No params found, scanning table')
                # resp = table.scan()
                # print('Table scanned, formulating json response')
                # data = resp['Items']
                raise Exception('No query parameters detected. Cannot retrieve all stats')
            else: 
                print('Params detected')        
                params = event["queryStringParameters"]
                year = CURRENT_YEAR
                if ('year' in params.keys()):
                    year = params['year'];
                print(params)
                if 'playerId' in params.keys():
                    playerId = params['playerId']
                    if 'round' in params.keys():
                        round_number = params['round']
                        print(f'Querying table for PlayerId {playerId} in round {round_number}')
                        # resp = table.get_item(
                        #     Key={
                        #         'player_id': playerId,
                        #         'round_number': round_number
                        #     }
                        # )
                        resp = table.get_item(
                            Key={
                                'pk': 'PLAYER#' + playerId,
                                'sk': f'STATS#{year}#' + round_number
                            }
                        )
                        data = resp['Item']                        
                    else:
                        print(f'Querying table for PlayerId {playerId}')
                        # resp = table.scan(
                        #     FilterExpression=Attr('player_id').eq(playerId)
                        # )
                        resp = table.query(
                            KeyConditionExpression=Key('pk').eq('PLAYER#' + playerId) & Key('sk').begins_with(f'STATS#{year}#')
                        )
                        data = resp['Items']
                elif 'nrlClub' in params.keys():
                    nrlClub = params['nrlClub']
                    if 'round' in params.keys():
                        round_number = params['round']
                        print(f'Querying table for {nrlClub} players in round {round_number}')
                        # resp = table.scan(
                        #     FilterExpression=Attr('nrl_club').eq(nrlClub) & Attr('round_number').eq(round_number)
                        # )
                        resp = table.query(
                            IndexName='sk-data-index',
                            KeyConditionExpression=Key('sk').eq(f'STATS#{year}#' + round_number) & Key('data').eq('CLUB#' + nrlClub)
                        )
                        data = resp['Items']
                        if 'LastEvaluatedKey' in resp.keys():
                            resp2 = table.query(
                                IndexName='sk-data-index',
                                KeyConditionExpression=Key('sk').eq(f'STATS#{year}#' + round_number) & Key('data').eq('CLUB#' + nrlClub),
                                ExclusiveStartKey=resp['LastEvaluatedKey']
                            )
                            data += resp2['Items']
                    else:
                        print(f'Querying table for {nrlClub} players in all rounds')
                        # resp = table.scan(
                        #     FilterExpression=Attr('nrlClub').eq(nrlClub)
                        # )
                        resp = table.scan(
                            FilterExpression=Attr('sk').begins_with(f'STATS#{year}#') & Attr('nrlClub').eq('CLUB#' + nrlClub)
                        )
                        data = resp['Items']
                elif 'round' in params.keys():
                    round_number = params['round']
                    print(f'Querying table for all stats from round {round_number}')
                    # resp = table.scan(
                    #     FilterExpression=Attr('round_number').eq(round_number)
                    # )
                    resp = table.query(
                        IndexName='sk-data-index',
                        KeyConditionExpression=Key('sk').eq(f'STATS#{year}#' + round_number) & Key('data').begins_with('CLUB#')
                    )
                    data = resp['Items']
                    if 'LastEvaluatedKey' in resp.keys():
                        # resp2 = table.scan(
                        #     FilterExpression=Attr('round_number').eq(round_number),
                        #     ExclusiveStartKey=resp['LastEvaluatedKey']
                        # )
                        resp2 = table.query(
                            IndexName='sk-data-index',
                            KeyConditionExpression=Key('sk').eq(f'STATS#{year}#' + round_number) & Key('data').begins_with('CLUB#'),
                            ExclusiveStartKey=resp['LastEvaluatedKey']
                        )
                        data += resp2['Items']
                    
                else:
                    print("Couldn't recognise parameter")
                    data = {"error": "Couldn't recognise parameter"}
            print('Table queried, returning json')
            return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(data))
                }
        if method == 'POST':
            body = json.loads(event['body'])
            round_number = body['round']
            player_ids = body['players']
            year = CURRENT_YEAR
            if ('year' in body.keys()):
                year = body['year']
            print(f'Loading stats for {len(player_ids)} players in round {round_number}')
            stats = []
            for player_id in player_ids:
                resp = table.get_item(Key={
                    'pk': player_id,
                    'sk': f'STATS#{year}#' + str(round_number)
                })
                if 'Item' in resp.keys():
                    stats.append(resp['Item'])
            return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(stats))
                }

    except Exception as e:
        return {
            'statusCode': 200,
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