import decimal
import json

import boto3
from boto3.dynamodb.conditions import Attr, Key

CURRENT_YEAR = 2025

dynamodbClient = boto3.client('dynamodb', 'ap-southeast-2')
dynamodbResource = boto3.resource('dynamodb', 'ap-southeast-2')
# round_table = dynamodbResource.Table('rounds2020')
table = dynamodbResource.Table('XRL2021')

def lambda_handler(event, context):
    try:
        method = event['httpMethod']
        if method == 'GET':
            if not event["queryStringParameters"]:
                # resp = table.scan()
                data = []
                for i in range(1, 28):
                    round_object = table.get_item(
                        Key={'pk': f'ROUND#{CURRENT_YEAR}#' + str(i), 'sk': 'STATUS'}
                    )
                    if ('Item') in round_object.keys():
                        round_object = round_object['Item']
                        round_object['fixtures'] = table.query(
                            KeyConditionExpression=Key('pk').eq(f'ROUND#{CURRENT_YEAR}#' + str(i)) & Key('sk').begins_with('FIXTURE')
                        )['Items']
                        data.append(round_object)
                return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(data))
                }
            print('Params detected')        
            params = event["queryStringParameters"]
            print(params)
            round_number = params['round']
            # resp = round_table.get_item(
            #     Key={
            #         'round_number': int(round_number)
            #     }
            # )
            data = table.get_item(
                Key={'pk': f'ROUND#{CURRENT_YEAR}#' + str(round_number), 'sk': 'STATUS'}
            )['Item']
            data['fixtures'] = table.query(
                KeyConditionExpression=Key('pk').eq(f'ROUND#{CURRENT_YEAR}#' + str(round_number)) & Key('sk').begins_with('FIXTURE')
            )['Items']
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
            operation = body['operation']
            if operation == 'get_current_round':
                active_rounds = table.query(
                    IndexName='sk-data-index',
                    KeyConditionExpression=Key('sk').eq('STATUS') & Key('data').eq('ACTIVE#true'),
                    FilterExpression=Attr('year').eq(CURRENT_YEAR)
                )['Items']
                round_number = max([r['round_number'] for r in active_rounds])
                data = [r for r in active_rounds if r['round_number'] == round_number][0]
                return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(data))
                }
            if operation == 'get_next_round':
                not_ongoing_rounds = table.query(
                    IndexName='sk-data-index',
                    KeyConditionExpression=Key('sk').eq('STATUS') & Key('data').begins_with('ACTIVE#'),
                    FilterExpression=Attr('year').eq(CURRENT_YEAR) & Attr('in_progress').eq(False)
                )['Items']
                round_number = min([r['round_number'] for r in not_ongoing_rounds])
                data = [r for r in not_ongoing_rounds if r['round_number'] == round_number][0]
                return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(data))
                }
            if operation == 'get_round_status':
                round_number = body['round_number']
                data = table.get_item(
                    Key={
                        'pk': f'ROUND#{CURRENT_YEAR}#' + round_number,
                        'sk': 'STATUS'
                    }
                )['Item']
                return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(data))
                }
            if operation == 'get_user_fixture':
                round_number = body['round_number']
                team_short = body['team_short']
                data = table.query(
                    KeyConditionExpression=Key('pk').eq(f'ROUND#{CURRENT_YEAR}#' + str(round_number)) & Key('sk').begins_with('FIXTURE#'),
                    FilterExpression=Attr('home').eq(team_short) | Attr('away').eq(team_short)
                )['Items']
                if len(data) > 0:
                    data = data[0]
                else:
                    data = {}
                return {
                    'statusCode': 200,
                    'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                    },
                    'body': json.dumps(replace_decimals(data))
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