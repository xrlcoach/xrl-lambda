import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

rounds = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('STATUS') & Key('data').eq('false'),
    FilterExpression=Attr('year').eq(2022)
)['Items']

for r in rounds:
  table.update_item(
    Key={
      'pk': r['pk'],
      'sk': r['sk']
    },
    UpdateExpression="set #D=:d",
    ExpressionAttributeNames={
      '#D': 'data'
    },
    ExpressionAttributeValues={
        ':d': 'ACTIVE#false',
    }
  )