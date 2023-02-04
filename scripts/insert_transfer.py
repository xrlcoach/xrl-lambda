import boto3
from boto3.dynamodb.conditions import Key, Attr
import math
from datetime import datetime

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

transfer_date = datetime.now()
table.put_item(
    Item={
        'pk': 'TRANSFER#freddyfittler' + str(transfer_date),
        'sk': 'TRANSFER',
        'data': 'ROUND#8',
        'user': 'freddyfittler',                        
        'datetime': transfer_date.strftime("%c"),
        'type': 'Drop',
        'round_number': 8,
        'player_id': '100026'
    }
)