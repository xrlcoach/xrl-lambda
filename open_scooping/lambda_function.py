import sys
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Attr, Key

CURRENT_YEAR = 2025

def lambda_handler(event, context):
    print(f"Script executing at {datetime.now()}")

    dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
    # rounds_table = dynamodb.Table('rounds2020')
    # players_table = dynamodb.Table('players2020')
    table = dynamodb.Table('XRL2021')

    print("Changing 'On Waivers' players to free agents")
    #Find all players who are labelled as 'On Waivers'
    on_waivers = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').eq('TEAM#On Waivers')
    )['Items']
    #Update each of those players to have XRL team of 'None'
    for player in on_waivers:
        table.update_item(
            Key={
                'pk': player['pk'],
                'sk': player['sk']
            },
            UpdateExpression="set #D=:d, xrl_team=:n",
            ExpressionAttributeNames={
                '#D': 'data'
            },
            ExpressionAttributeValues={
                ':d': 'TEAM#None',
                ':n': 'None' 
            }
        )

    print("Changing 'Pre-Waivers' players to 'On Waivers'")
    #Find all players who are labelled as 'Pre-Waivers'
    pre_waivers = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').eq('TEAM#Pre-Waivers')
    )['Items']
    #Update those players to be 'On Waivers'
    for player in pre_waivers:
        table.update_item(
            Key={
                'pk': player['pk'],
                'sk': player['sk']
            },
            UpdateExpression="set #D=:d, xrl_team=:n",
            ExpressionAttributeNames={
                '#D': 'data'
            },
            ExpressionAttributeValues={
                ':d': 'TEAM#On Waivers',
                ':n': 'On Waivers' 
            }
        )

    #Find all active rounds
    resp = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('STATUS') & Key('data').eq('ACTIVE#true'),
        FilterExpression=Attr('year').eq(CURRENT_YEAR)
    )
    #Find the current active round
    round_number = max([r['round_number'] for r in resp['Items']])

    print(f"Current round: {round_number}. Setting 'scooping' to true")
    #Update round to open scooping
    table.update_item(
        Key={
            'pk': f'ROUND#{CURRENT_YEAR}#' + str(round_number),
            'sk': 'STATUS'
        },
        UpdateExpression="set scooping=:t",
        ExpressionAttributeValues={
            ':t': True
        }
    )
    print(f"Player scooping is now open.")