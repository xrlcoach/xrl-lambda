import boto3
from boto3.dynamodb.conditions import Key, Attr
import sys
import math



dynamodbResource = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodbResource.Table('XRL2021')

jake_arthur = table.get_item(Key={
  'pk': 'PLAYER#100669',
  'sk': 'PROFILE'
})['Item']
jakob_arthur = table.get_item(Key={
  'pk': 'PLAYER#100633',
  'sk': 'PROFILE'
})['Item']

print(jake_arthur)
combined_stats = {key: jake_arthur['stats'][key] + jakob_arthur['stats'][key] for key in jake_arthur['stats'].keys()}
combined_pm_stats = {key: jake_arthur['scoring_stats']['Playmaker'][key] + jakob_arthur['scoring_stats']['Playmaker'][key] for key in jake_arthur['scoring_stats']['Playmaker'].keys()}
combined_scoring_stats = {
  "kicker": jake_arthur['scoring_stats']['kicker'],
  "Playmaker": combined_pm_stats
}
table.update_item(
    Key={
        'pk': jake_arthur['pk'],
        'sk': jake_arthur['sk']
    },
    UpdateExpression="set stats=:s, scoring_stats=:ss",
    ExpressionAttributeValues={
        ':s': combined_stats,
        ':ss': combined_scoring_stats
    }
)
print('Merge Complete')
