import boto3
from boto3.dynamodb.conditions import Attr, Key

session = boto3.Session(profile_name='jamesedchristie')

dynamodb = session.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

squads = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
)['Items']

players_with_mismatch = [p for p in squads if (p['position'] not in (p['scoring_stats'].keys()))]

for player in players_with_mismatch:
	print(f"Player {player['player_name']} has position {player['position']} but scoring stats {player['scoring_stats'].keys()}")
	# table.update_item(
	# 	Key={
	# 		'pk': player['pk'],
	# 		'sk': player['sk']
	# 	},
	# 	UpdateExpression="set #ss=:s",
	# 	ExpressionAttributeNames={
	# 		'#ss': 'scoring_stats'
	# 	},
	# 	ExpressionAttributeValues={
	# 		':s': {
	# 			player['position']: {},
	# 			'kicker': {}
	# 		}
	# 	}	)

players_on_waivers = [p for p in squads if p['xrl_team'] == 'On Waivers']

for player in players_on_waivers:
	print(f"Player {player['player_name']} is on waivers but should be a free agent")
	# table.update_item(
	# 	Key={
	# 		'pk': player['pk'],
	# 		'sk': player['sk']
	# 	},
	# 	UpdateExpression="set #D=:d, xrl_team=:t",
	# 	ExpressionAttributeNames={
	# 		'#D': 'data'
	# 	},
	# 	ExpressionAttributeValues={
	# 		':d': 'TEAM#None',
	# 		':t': 'None',
	# 	}	
	# )

players_listed_as_tigers = [p for p in squads if p['nrl_club'] == 'Tigers']

for player in players_listed_as_tigers:
	print(f"Player {player['player_name']} is listed as Tigers but should be Wests Tigers")
	# table.update_item(
	# 	Key={
	# 		'pk': player['pk'],
	# 		'sk': player['sk']		
	# 	},
	# 	UpdateExpression="set nrl_club=:c",
	# 	ExpressionAttributeValues={
	# 		':c': 'Wests Tigers',
	# 	}	
	# )