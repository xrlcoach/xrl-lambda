import boto3
from boto3.dynamodb.conditions import Key, Attr
import csv

dynamodb = boto3.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

squads = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
)['Items']
max_player_id = max([int(p['player_id']) for p in squads])

with open('data/PlayerList2022.csv', 'r') as player_list:
    reader = csv.reader(player_list)
    new_players = 0
    existing_players = 0
    total_players = 0
    players = []
    for row in reader:
      if row[0] == 'Player':
        continue
      player = {
        'player_name': row[0],
        'search_name': row[0].lower(),
        'nrl_club': row[1],
        'position': row[2]
      }
      players.append(player)
      db_record = None
      name_matches = [p for p in squads if p['search_name'] == player['search_name']]
      if len(name_matches) > 0:
        if len(name_matches) > 1:
          print(f"{len(name_matches)} players found with name {player['search_name']}")
          db_record = 2
        else:
          db_record = name_matches[0]
          table.update_item(
            Key={
              'pk': db_record['pk'],
              'sk': db_record['sk']
            },
            UpdateExpression="set nrl_club=:c, #P=:p",
            ExpressionAttributeNames={
              '#P': 'position'
            },
            ExpressionAttributeValues={
                ':c': player['nrl_club'],
                ':p': player['position']
            }
          )
      if db_record != None:
        existing_players += 1
      else:
        new_players += 1
        new_player_id = str(max_player_id + 1)
        print(f"New player: {player['player_name']}, player_id {new_player_id}")
        max_player_id += 1
        player_entry = {
          'pk': 'PLAYER#' + new_player_id,
          'sk': 'PROFILE',
          'data': 'TEAM#On Waivers',
          'player_id': new_player_id,
          'player_name': player['player_name'],
          'nrl_club': player['nrl_club'],
          'xrl_team': 'On Waivers',
          'search_name': player['search_name'],
          'position': player['position'],
          'position2': None,
          'stats': {},
          'scoring_stats': {
              player['position']: {},
              'kicker': {}
          },
          'times_as_captain': 0,
          'new_position_appearances': {}
        }
        table.put_item(Item=player_entry)
      total_players += 1
    print(f"Total players: {total_players}")
    print(f"Existing players: {existing_players}")
    print(f"New players: {new_players}")
