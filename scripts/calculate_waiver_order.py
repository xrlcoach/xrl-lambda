import boto3
from boto3.dynamodb.conditions import Key, Attr
import math
from datetime import datetime

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

users = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').begins_with('NAME#')
)['Items']

#Sort users by waiver rank
waiver_order = sorted(users, key=lambda u: u['waiver_rank'])

print("Current waiver order:")
for rank, user in enumerate(waiver_order, 1):
    print(f"{rank}. {user['team_name']}")

# users_who_picked = [u for u in waiver_order if u['players_picked'] > 0]    

# #Recalculate waiver order (players who didn't pick followed by those who did in reverse order)
# waiver_order = [u for u in waiver_order if u not in users_who_picked] + users_who_picked[::-1]

# #Save new waiver order to db 
# print("New waiver order:")
# for rank, user in enumerate(waiver_order, 1):
#     print(f"{rank}. {user['team_name']}")
    
#     table.update_item(
#         Key={
#             'pk': user['pk'],
#             'sk': 'DETAILS'
#         },
#         UpdateExpression="set waiver_rank=:wr",
#         ExpressionAttributeValues={
#             ':wr': rank
#         }
#     )