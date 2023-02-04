import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

CURRENT_YEAR = 2022

class PlayersGetRequest: 
    def __init__(self, params):
        self.nrlClub = params['nrlClub'] if 'nrlClub' in params.keys() else None
        self.xrlTeam = params['xrlTeam'] if 'xrlTeam' in params.keys() else None
        self.playerId = params['playerId'] if 'playerId' in params.keys() else None
        self.news = params['news'] if 'news' in params.keys() else None
        self.year = int(params['year']) if 'year' in params.keys() else CURRENT_YEAR

params = PlayersGetRequest({ 'year': '2021 '})

players = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
)['Items']

yearstats = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq(f'YEARSTATS#{params.year}') & Key('data').begins_with('PLAYER_NAME')
)['Items']

for player in players:
  player_stats = next((stats for stats in yearstats if stats['pk'] == player['pk']), None)
  if player_stats == None:
      print(f'No stats: {player["player_name"]}')
  else:
      player['stats'] = player_stats['stats']
      player['scoring_stats'] = player_stats['scoring_stats']
      if player['pk'] == 'PLAYER#100012':
        print('Brimson')
        print(player)