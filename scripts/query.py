import boto3
from boto3.dynamodb.conditions import Key, Attr
import decimal
from utils import replace_decimals

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
# table = dynamodb.Table('users2020')
table = dynamodb.Table('XRL2021')

# for i in range(1, 26):
# lineup_entries = table.query(
#     IndexName='sk-data-index',
#     KeyConditionExpression=Key('sk').eq(
#         'LINEUP#' + str(i)) & Key('data').begins_with('TEAM'),
# )['Items']
# with table.batch_writer() as batch:
#     for entry in lineup_entries:
#         entry['sk'] = 'LINEUP#2021#' + str(i)
#         table.put_item(Item=entry)

# stats_entries = table.query(
#     IndexName='sk-data-index',
#     KeyConditionExpression=Key('sk').eq(
#         'STATS#' + str(i)) & Key('data').begins_with('CLUB'),
# )['Items']
# print(len(stats_entries))
# with table.batch_writer() as batch:
#     for entry in stats_entries:
#         entry['sk'] = 'STATS#2021#' + str(i)
#         table.put_item(Item=entry)


for i in range (22, 26):
    table.put_item(
        Item={
            'pk': 'ROUND#2022#' + str(i),
            'sk': 'STATUS',
            'data': 'ACTIVE#' + 'true' if i == 1 else 'false',
            'active': True if i == 1 else False,
            'scooping': True if i == 1 else False,
            'in_progress': False,
            'completed': False,
            'round_number': i,
            'year': 2022
        }
    )

# fixtures = table.query(
#         KeyConditionExpression=Key('pk').eq(
#             'ROUND#2021#1') & Key('sk').begins_with('FIXTURE'),
#     )['Items']
# for fixture in fixtures:
#     fixture['pk'] = 'ROUND#2022#1'
#     fixture['home_score'] = 0
#     fixture['away_score'] = 0
#     table.put_item(Item=fixture)

# waivers = table.query(KeyConditionExpression=Key('pk').eq(
#             'WAIVER') & Key('sk').begins_with('REPORT'),
#     )['Items']

# for report in waivers:
#     round_number = report['waiver_round'].split('_')[0]
#     day = report['waiver_round'].split('_')[1]
#     report['round_number'] = round_number
#     report['day'] = day
#     report['year'] = 2021
#     report['sk'] = 'REPORT#2021#' + report['waiver_round']
#     table.put_item(Item=report)

# print(replace_decimals(resp['Items']))

# users = table.query(IndexName='sk-data-index', KeyConditionExpression=Key('sk').eq(
#     'DETAILS') & Key('data').begins_with('NAME'))['Items']

# for user in users:
#     table.put_item(Item={
#         'pk': user['pk'],
#         'sk': 'YEARSTATS#2022',
#         'data': user['data'],
#         'username': user['username'],
#         'stats': {
#             'against': 0,
#             'draws': 0,
#             'for': 0,
#             'losses': 0,
#             'points': 0,
#             'wins': 0,
#         },
#         'year': 2022
#     })
