"""
Recreate YEARSTATS records for a previous season from per-round STATS and FIXTURE data.

Use this when reset_season_stats.py was run before setup_new_season.py archived the stats.
The per-round appearance records (STATS#year#round) and fixture results should still exist.
"""

import math

import boto3
from boto3.dynamodb.conditions import Key

session = boto3.Session(profile_name='default')
dynamodb = session.resource('dynamodb', 'ap-southeast-2')
table = dynamodb.Table('XRL2021')

YEAR_TO_RECREATE = 2025
PLAYER_ROUNDS = 26
FIXTURE_ROUNDS = 22
DRY_RUN = False  # Set to False to actually write to DynamoDB


# ── 1. Collect all player appearance stats for the season ──

print(f"Collecting all appearance stats for {YEAR_TO_RECREATE}...")
all_stats = []
for rnd in range(1, PLAYER_ROUNDS + 1):
    items = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq(f'STATS#{YEAR_TO_RECREATE}#{rnd}') & Key('data').begins_with('CLUB#')
    )['Items']
    if items:
        print(f"  Round {rnd}: {len(items)} appearances")
        all_stats += items
    else:
        print(f"  Round {rnd}: no data, stopping round scan")
        break

print(f"Total appearances found: {len(all_stats)}")

# Build a set of all player IDs that had appearances
player_ids = set(stat['player_id'] for stat in all_stats)
print(f"Unique players with appearances: {len(player_ids)}")


# ── 2. Get player profiles (for name/search_name metadata) ──

print("Fetching player profiles...")
profiles = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
)['Items']
profile_map = {p['player_id']: p for p in profiles}
print(f"  {len(profiles)} profiles loaded")


# ── 3. Accumulate player stats ──

print("Accumulating player stats...")
player_year_stats = {}

for player_id in player_ids:
    appearances = [s for s in all_stats if s['player_id'] == player_id]
    accumulated = {
        'stats': {'appearances': len(appearances)},
        'scoring_stats': {}
    }

    for app in appearances:
        # Raw stats
        for stat, value in app['stats'].items():
            if isinstance(value, (str, dict)):
                continue
            if value % 1 != 0:
                continue
            if stat not in accumulated['stats']:
                accumulated['stats'][stat] = 0
            accumulated['stats'][stat] += value

        # Scoring stats (by position + kicker)
        for position, pos_stats in app['scoring_stats'].items():
            if position not in accumulated['scoring_stats']:
                accumulated['scoring_stats'][position] = {}
            for stat, value in pos_stats.items():
                if stat not in accumulated['scoring_stats'][position]:
                    accumulated['scoring_stats'][position][stat] = 0
                if stat == 'send_offs':
                    if 'send_off_deduction' not in accumulated['scoring_stats'][position]:
                        accumulated['scoring_stats'][position]['send_off_deduction'] = 0
                    if value != 0:
                        accumulated['scoring_stats'][position][stat] += 1
                        minutes = 80 - int(value)
                        deduction = math.floor(minutes / 10) + 4
                        accumulated['scoring_stats'][position]['send_off_deduction'] += deduction
                else:
                    accumulated['scoring_stats'][position][stat] += value

    # Calculate points totals
    for position, pos_stats in accumulated['scoring_stats'].items():
        if position == 'kicker':
            pos_stats['points'] = pos_stats.get('goals', 0) * 2
        else:
            pos_stats['points'] = pos_stats.get('tries', 0) * 4
            pos_stats['points'] += pos_stats.get('field_goals', 0)
            pos_stats['points'] += pos_stats.get('2point_field_goals', 0) * 2
            pos_stats['points'] += pos_stats.get('involvement_try', 0) * 4
            pos_stats['points'] += pos_stats.get('positional_try', 0) * 4
            pos_stats['points'] -= pos_stats.get('mia', 0) * 4
            pos_stats['points'] -= pos_stats.get('concede', 0) * 4
            pos_stats['points'] -= pos_stats.get('sin_bins', 0) * 2
            pos_stats['points'] -= pos_stats.get('send_off_deduction', 0)

    player_year_stats[player_id] = accumulated

print(f"  Accumulated stats for {len(player_year_stats)} players")


# ── 4. Recreate user/team stats from fixture results ──

print(f"Collecting fixture results for {YEAR_TO_RECREATE}...")
users = table.query(
    IndexName='sk-data-index',
    KeyConditionExpression=Key('sk').eq('DETAILS') & Key('data').begins_with('NAME#')
)['Items']

user_stats = {}
for user in users:
    user_stats[user['team_short']] = {
        'username': user['username'],
        'team_short': user['team_short'],
        'pk': user['pk'],
        'stats': {
            'wins': 0,
            'losses': 0,
            'draws': 0,
            'points': 0,
            'for': 0,
            'against': 0,
        }
    }

for rnd in range(1, FIXTURE_ROUNDS + 1):
    fixtures = table.query(
        KeyConditionExpression=Key('pk').eq(f'ROUND#{YEAR_TO_RECREATE}#{rnd}') & Key('sk').begins_with('FIXTURE')
    )['Items']
    if not fixtures:
        print(f"  Round {rnd}: no fixtures, stopping")
        break
    # Only count completed fixtures
    completed = [f for f in fixtures if f.get('data') == 'COMPLETED#true']
    if not completed:
        print(f"  Round {rnd}: no completed fixtures, stopping")
        break
    print(f"  Round {rnd}: {len(completed)} completed fixtures")
    for match in completed:
        home = match['home']
        away = match['away']
        home_score = int(match.get('home_score', 0))
        away_score = int(match.get('away_score', 0))

        if home not in user_stats or away not in user_stats:
            print(f"    WARNING: team {home} or {away} not found in users, skipping fixture")
            continue

        user_stats[home]['stats']['for'] += home_score
        user_stats[home]['stats']['against'] += away_score
        user_stats[away]['stats']['for'] += away_score
        user_stats[away]['stats']['against'] += home_score

        if home_score > away_score:
            user_stats[home]['stats']['wins'] += 1
            user_stats[home]['stats']['points'] += 2
            user_stats[away]['stats']['losses'] += 1
        elif away_score > home_score:
            user_stats[away]['stats']['wins'] += 1
            user_stats[away]['stats']['points'] += 2
            user_stats[home]['stats']['losses'] += 1
        else:
            user_stats[home]['stats']['draws'] += 1
            user_stats[home]['stats']['points'] += 1
            user_stats[away]['stats']['draws'] += 1
            user_stats[away]['stats']['points'] += 1


# ── 5. Write YEARSTATS records ──

print("")
print("=== Summary ===")
print(f"Player YEARSTATS to write: {len(player_year_stats)}")
print(f"User YEARSTATS to write: {len(user_stats)}")
print("")

if DRY_RUN:
    print("DRY RUN — showing what would be written:")
    print("")

    print("--- Sample Player YEARSTATS (first 3) ---")
    for i, (player_id, stats) in enumerate(player_year_stats.items()):
        if i >= 3:
            break
        profile = profile_map.get(player_id, {})
        name = profile.get('player_name', player_id)
        apps = stats['stats'].get('appearances', 0)
        positions = list(stats['scoring_stats'].keys())
        print(f"  {name}: {apps} appearances, positions: {positions}")

    print("")
    print("--- User YEARSTATS ---")
    for team, data in sorted(user_stats.items()):
        s = data['stats']
        print(f"  {team}: W{s['wins']} L{s['losses']} D{s['draws']} Pts{s['points']} F{s['for']} A{s['against']}")

    print("")
    print("Set DRY_RUN = False to write these records to DynamoDB.")
else:
    print("Writing player YEARSTATS records...")
    written = 0
    for player_id, stats in player_year_stats.items():
        profile = profile_map.get(player_id, {})
        player_name = profile.get('player_name', 'Unknown')
        search_name = profile.get('search_name', player_name.lower().replace(' ', ''))

        table.put_item(
            Item={
                'pk': f'PLAYER#{player_id}',
                'sk': f'YEARSTATS#{YEAR_TO_RECREATE}',
                'data': f'PLAYER_NAME#{player_name}',
                'player_name': player_name,
                'search_name': search_name,
                'year': YEAR_TO_RECREATE,
                'stats': stats['stats'],
                'scoring_stats': stats['scoring_stats'],
            }
        )
        written += 1
    print(f"  Wrote {written} player YEARSTATS records")

    print("Writing user YEARSTATS records...")
    written = 0
    for team, data in user_stats.items():
        table.put_item(
            Item={
                'pk': data['pk'],
                'sk': f'YEARSTATS#{YEAR_TO_RECREATE}',
                'data': f'TEAM#{data["team_short"]}',
                'username': data['username'],
                'stats': data['stats'],
                'year': YEAR_TO_RECREATE,
            }
        )
        written += 1
    print(f"  Wrote {written} user YEARSTATS records")

print("")
print("Done!")
