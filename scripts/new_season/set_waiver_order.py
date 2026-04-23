import boto3
from boto3.dynamodb.conditions import Attr, Key

session = boto3.Session(profile_name='jamesedchristie')

dynamodb = session.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

waiver_order = [
	'BWS',
	'PUN',
	'BOX',
	'WOL',
	'XIII',
	'DRU',
	'MIN',
	'GUN',
	'RAM',
	'ROX',
	'MCD',
	'COU',
	'CBT',
	'MON',
]

users = table.query(
		IndexName='sk-data-index',
		KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').begins_with('NAME#')
	)['Items']

for user in users:
	team_short = user['team_short']
	if team_short in waiver_order:
		new_rank = waiver_order.index(team_short) + 1
		print(f"Setting waiver rank for {user['team_name']} to {new_rank}")
		# table.update_item(
		# 	Key={
		# 		'pk': user['pk'],
		# 		'sk': 'DETAILS'
		# 	},
		# 	UpdateExpression="set waiver_rank=:wr",
		# 	ExpressionAttributeValues={
		# 		':wr': new_rank
		# 	}
		# )
