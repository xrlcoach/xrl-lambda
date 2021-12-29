import boto3
from boto3.dynamodb.conditions import Key, Attr
import decimal
from utils import replace_decimals

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
# table = dynamodb.Table('users2020')
table = dynamodb.Table('XRL2021')

# resp = table.query(
#       IndexName='sk-data-index',
#       KeyConditionExpression=Key('sk').eq(
#           'LINEUP#1') & Key('data').begins_with('TEAM#'),
#       FilterExpression=Attr('year').eq(2022),
#       Select='COUNT'
#   )

resp = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq(
        'STATUS') & Key('data').eq('ACTIVE#true'),
    FilterExpression=Attr('year').eq(2021)
)

print(replace_decimals(resp['Items']))
