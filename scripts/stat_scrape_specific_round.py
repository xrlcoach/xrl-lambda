import json
import math
import os
import stat
import sys
import time
from datetime import date, datetime, time
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.errorfactory import ClientError
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.expected_conditions import \
    presence_of_element_located
from selenium.webdriver.support.ui import WebDriverWait

#region Global variables
CURRENT_YEAR = 2025
forwards = ['Prop', '2nd Row', '2nd', 'Lock']
playmakers = ['Five-Eighth', 'Halfback', 'Hooker']
backs = ['Winger', 'Centre', 'Fullback']
#endregion

#region Functions for calculating player XRL scores

def involvement_try(player, position):
    #=IF(AND(D2="Back",AB2>34),1,IF(AND(D2="Playmaker",AB2>44),1,IF(AND(D2="Forward",AB2>49),1,0)))
    relevant_stats = ["Tries", "1 Point Field Goals", "2 Point Field Goals", "All Runs", "Line Breaks", "Line Break Assists", "Try Assists", "Tackle Breaks",
        "Offloads", "Tackles Made", "Kicks", "40/20", "20/40", "One on One Steal"]
    stats = sum([player[stat] for stat in player.keys() if stat in relevant_stats])
    if position == 'Back' and stats > 34:
        return True
    elif position == 'Playmaker' and stats > 44:
        return True
    elif position == 'Forward' and stats > 44:
        return True
    return False 

def positional_try(player, position):
    #=IF(S2>7,1,IF(D2="Back",IF(L2>16,1,0),IF(T2>39,1,IF(D2="Playmaker",IF(W2>249,1,0),IF(D2="Forward",IF(M2>139,1,0),0)))))
    tries = 0
    relevant_stats = ["Line Breaks", "Line Break Assists", "Try Assists", "Tackle Breaks",
        "Offloads", "40/20", "20/40", "One on One Steal"]
    creative = sum([player[stat] for stat in player.keys() if stat in relevant_stats])
    if creative > 7:
        tries += 1
    if position == 'Back':
        if player["All Runs"] > 16:
            tries += 1
    if position == 'Playmaker':
        if player["Tackles Made"] > 39:
            tries += 1
        if player["Kicking Metres"] > 249:
            tries += 1
    if position == 'Forward':
        if player["Tackles Made"] > 39:
            tries += 1
        if player["All Run Metres"] > 119:
            tries += 1
    return tries

def missing(player, position):
    #=IF(AND(F2>49,G2<2),IF(AND(D2="Back",AB2<15),1,IF(AND(D2="Playmaker",AB2<20),1,IF(AND(D2="Forward",AB2<25),1,0))),0)
    relevant_stats = ["Tries", "1 Point Field Goals", "2 Point Field Goals", "All Runs", "Line Breaks", "Line Break Assists", "Try Assists", "Tackle Breaks",
        "Offloads", "Tackles Made", "Kicks", "40/20", "20/40"]
    stats = sum([player[stat] for stat in player.keys() if stat in relevant_stats])
    if player["Mins Played"] > 49 and player["Tries"] < 2:
        if position == 'Back' and stats < 15:
            return True
        if position == 'Playmaker' and stats < 20:
            return True
        if position == 'Forward' and stats < 25:
            return True
    return False

#endregion

def get_stats():
    # Start timer to see how long things take
    start = datetime.now()
    print(f"Script executing at {start}")

    # Initiate database connection
    dynamodbResource = boto3.resource('dynamodb', 'ap-southeast-2')   
    table = dynamodbResource.Table('XRL2021')

    # Get current active round
    target_round_number = sys.argv[1]

    # Get all player profiles
    squads = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('PROFILE') & Key('data').begins_with('TEAM')
    )['Items']

    # Initiate stat columns
    stat_columns_final = []

    # chrome_options = webdriver.ChromeOptions()
    options = Options()
    options.headless = True
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--single-process')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--log-level=3')
    options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

    driver = webdriver.Chrome(
        executable_path='../../../chromedriver', options=options
    )
    #draw_url = 'https://www.nrl.com/draw/'
    draw_url = f'https://www.nrl.com/draw/?competition=111&season={CURRENT_YEAR}&round={target_round_number}'
    match_url_base = f'https://www.nrl.com/draw/nrl-premiership/{CURRENT_YEAR}/'

    # Set timeout time
    wait = WebDriverWait(driver, 10)
    # retrive URL in headless browser
    print(f"Connecting to {draw_url}")
    driver.get(draw_url)

    wait.until(presence_of_element_located((By.ID, "vue-draw")))
    print("Found vue-data element")
    # wait.until(presence_of_element_located((By.ID, "rounds-filter-button")))
    
    round_number = driver.find_element_by_css_selector(
        "button[class='filter-round__button filter-round__button--dropdown']"
        ).text
    round_number = round_number.split()
    print(round_number)
    number = round_number[1]
    round_number = "-".join(round_number)

    player_stats_final = []

    # Scrape match titles located in hidden html fields
    draw_list = driver.find_elements_by_class_name("u-visually-hidden")
    matches = []
    for match in draw_list:
        if match.text[:6] == 'Match:':
            # Format match title into url
            fixture = match.text[7:].split(' vs ')
            fixture_formatted = []
            for team in fixture:
                words = team.split()
                team_name = "-".join(words)
                fixture_formatted.append(team_name)
            fixture_formatted = "-v-".join(fixture_formatted)
            fixture_url = match_url_base + f'{round_number}/{fixture_formatted}'
            matches.append(fixture_url)
    
    match_count = 0

    for match in matches:
        
        match_count += 1

        # Change URL into match title and team names
        title = match[match.rfind('/') + 1:]
        title = title.replace('-', ' ')
        teams = title.split(' v ')
        home_team = teams[0]
        away_team = teams[1]

        print(f'\u001b[32mGetting player stats for {title}\u001b[0m')
        # Send browser to match url
        driver.get(match)

        # PUT SEND OFF SCRAPING HERE
        send_offs = {}
        divs = driver.find_elements_by_class_name('u-display-flex')
        for div in divs:
            try:
                h4 = div.find_element_by_tag_name('h4')
            except NoSuchElementException:
                continue
            if "sendOff" in h4.text:
                uls = div.find_elements_by_tag_name('ul')
                for ul in uls:
                    lis = ul.find_elements_by_tag_name('li')
                    for li in lis:
                        print("Red card: " + li.text)
                        split = li.text.split()
                        name = ' '.join(split[:-1])
                        minute = split[-1][:-1]                        
                        send_offs[name] = int(minute)

        # find player stats
        try:
            player_stats = driver.find_element(By.LINK_TEXT, "Player Stats")
        except NoSuchElementException:
            print(f"\u001b[31mCouldn't get player stats for {title}\u001b[0m")
            continue
        player_stats.send_keys(Keys.RETURN)

        wait.until(presence_of_element_located((By.ID, "tabs-match-centre-3")))
        
        # Find head of table with column names and parse into stat_columns list
        if match_count == 1:
            head = driver.find_element_by_tag_name("thead")
            stats_row = head.find_elements_by_tag_name("th")

            stat_columns = []
            for col in stats_row:
                stat_columns.append(col.text)

            stat_columns = [stat for stat in stat_columns if stat != '']
            stat_columns = stat_columns[11:]
            stat_columns_final = stat_columns
            

        # Scrape player stats into list
        home_file = []

        home_stats = driver.find_elements_by_class_name('table-tbody__tr')  
        for player in home_stats:
            home_file.append(player.text)

        # Press button for away team
        away_stats_button = f"button[class='toggle-group__item u-flex-center t-{away_team.lower().replace(' ', '-')}']"
        driver.execute_script("arguments[0].click();", driver.find_element_by_css_selector(away_stats_button))

        away_file = []

        # Scrape player stats for away team
        away_stats = driver.find_elements_by_class_name('table-tbody__tr') 
        for player in away_stats:
            away_file.append(player.text)

        home_stats = []
        home_players = []

        # Go through list of rows and append stats (starting with digit) to stats
        # and player names to players, ignoring blank entries
        for row in home_file:
            if len(row) > 0:
                if row[0].isdigit():
                    home_stats.append(row)
                elif row[0].isalpha():
                    home_players.append(row)

        away_stats = []
        away_players = []  

        for row in away_file:
            if len(row) > 0:
                if row[0].isdigit():
                    away_stats.append(row)
                elif row[0].isalpha():
                    away_players.append(row) 

        # Create final lists for player stats, converting column info to correct type and format

        for i in range(len(home_players)):
            player = []
            player.append({'player_name': home_players[i], 'nrl_club': home_team, 'opponent': away_team})
            stat_map = {}
            ps = home_stats[i].split()
            if len(ps) > len(stat_columns_final):
                ps.remove('Row')
            for j in range(len(ps)):
                if j == 1:
                    stat_map[stat_columns_final[j]] = ps[j]
                elif ':' in ps[j]:
                    stat_map[stat_columns_final[j]] = int(ps[j][:2])
                elif '%' in ps[j]:
                    stat_map[stat_columns_final[j]] = Decimal(str(float(ps[j][:-1]) / 100))
                elif '.' in ps[j]:
                    if ps[j][-1] == 's':
                        stat_map[stat_columns_final[j]] = Decimal(str(ps[j][:-1]))
                    else:
                        stat_map[stat_columns_final[j]] = Decimal(str(ps[j]))
                elif ps[j] == '-':
                    stat_map[stat_columns_final[j]] = 0
                elif ps[j] == '2nd':
                    stat_map[stat_columns_final[j]] = '2nd Row'
                elif ps[j] == 'Row':
                    continue
                else:
                    try:
                        stat_map[stat_columns_final[j]] = int(ps[j])
                    except ValueError:
                        stat_map[stat_columns_final[j]] = ps[j]
            if home_players[i] in send_offs.keys():
                stat_map['Send Offs'] = send_offs[home_players[i]]
            player.append(stat_map)
            player_stats_final.append(player)

        for i in range(len(away_players)):
            player = []
            player.append({'player_name': away_players[i], 'nrl_club': away_team, 'opponent': home_team})
            stat_map = {}
            ps = away_stats[i].split()
            if len(ps) > len(stat_columns_final):
                ps.remove('Row')
            for j in range(len(ps)):
                if j == 1:
                    stat_map[stat_columns_final[j]] = ps[j]
                elif ':' in ps[j]:
                    stat_map[stat_columns_final[j]] = int(ps[j][:2])
                elif '%' in ps[j]:
                    stat_map[stat_columns_final[j]] = Decimal(str(float(ps[j][:-1]) / 100))
                elif '.' in ps[j]:
                    if ps[j][-1] == 's':
                        stat_map[stat_columns_final[j]] = Decimal(str(ps[j][:-1]))
                    else:
                        stat_map[stat_columns_final[j]] = Decimal(str(ps[j]))
                elif ps[j] == '-':
                    stat_map[stat_columns_final[j]] = 0
                elif ps[j] == '2nd':
                    stat_map[stat_columns_final[j]] = '2nd Row'
                elif ps[j] == 'Row':
                    continue
                else:
                    try:
                        stat_map[stat_columns_final[j]] = int(ps[j])
                    except ValueError:
                        stat_map[stat_columns_final[j]] = ps[j]
            if away_players[i] in send_offs.keys():
                stat_map['Send Offs'] = send_offs[away_players[i]]
            player.append(stat_map)
            player_stats_final.append(player)

      

    # must close the driver after task finished
    driver.close()
    driver.quit()

    print(f"Recorded {len(player_stats_final)} player appearances.")
    print("Stat scraping complete. Calculating player scores...")
    
    for player in player_stats_final:
        squad_entry = [p for p in squads if p['player_name'].lower() == player[0]['player_name'].lower() and p['nrl_club'] == player[0]['nrl_club']]
        if len(squad_entry) == 0:
            message = "Update: "
            squad_entry = [p for p in squads if p['player_name'].lower() == player[0]['player_name'].lower()]
            if len(squad_entry) != 1:
                if len(squad_entry) == 0:
                    print(f"Couldn't find {player[0]['player_name']} in database. Adding now. Remember to check position later.")
                    message += f"{player[0]['player_name']} made his debut for the {player[0]['nrl_club']}."
                elif len(squad_entry) > 1:
                    print(f"""A player named {player[0]['player_name']} has moved to the {player[0]['nrl_club']}. There is more than one
                    {player[0]['player_name']} in the database. Creating a new player entry for now. Remember to update the player's
                    team manually and update stats again.""")
                    message += f"""{player[0]['player_name']} has moved to the {player[0]['nrl_club']}. Question is, which {player[0]['player_name']} is this?"""
                if player[1]['Position'] in forwards: new_player_position = 'Forward'
                elif player[1]['Position'] in playmakers: new_player_position = 'Playmaker'
                elif player[1]['Position'] in backs: new_player_position = 'Back'
                else:
                    new_player_position = 'Forward'
                    print(f"Couldn't determine {player[0]['player_name']}'s posiion.")
                    message += f"""\n{player[0]['player_name']} started on the interchange bench. Asked what his best position is, {player[0]['nrl_club']}'s coach said 'We're still not sure yet.'"""
                player_id = str(max([int(p['player_id']) for p in squads]) + 1)
                # Convert last name to uppercase
                split = player[0]['player_name'].split(' ')
                first_name = split[0]
                last_name = ' '.join(split[1:])
                last_name = last_name.upper()
                new_player_name = ' '.join([first_name, last_name])
                table.put_item(
                    Item={
                        'pk': 'PLAYER#' + player_id,
                        'sk': 'PROFILE',
                        'data': 'TEAM#None',
                        'player_id': player_id,
                        'player_name': new_player_name,
                        'nrl_club': player[0]['nrl_club'],
                        'xrl_team': 'None',
                        'search_name': new_player_name.lower(),
                        'position': new_player_position,
                        'position2': None,
                        'stats': {},
                        'scoring_stats': {
                            new_player_position: {
                                'tries': 0,
                                'involvement_try': 0,
                                'positional_try': 0,
                                'mia': 0,
                                'concede': 0,
                                'sin_bins': 0,
                                'send_offs': 0,
                                'field_goals': 0,
                                'points': 0
                            },
                            'kicker': {
                                'goals': 0,
                                'points': 0
                            }
                        },
                        'times_as_captain': 0,
                        'new_position_appearances': {}
                    }                    
                )
                squad_entry = {}
                squad_entry['position'] = new_player_position
                squad_entry['position2'] = ''
            else:
                squad_entry = squad_entry[0]
                player_id = squad_entry['player_id']
                print(f"{player[0]['player_name']} has moved to the {player[0]['nrl_club']}. Updating his team in the database.")
                message += f"{player[0]['player_name']} has moved to the {player[0]['nrl_club']}."
                table.update_item(
                    Key={
                        'pk': squad_entry['pk'],
                        'sk': 'PROFILE'
                    },
                    UpdateExpression="set nrl_club=:c",
                    ExpressionAttributeValues={
                        ':c': player[0]['nrl_club']
                    }
                )
                table.update_item(
                    Key={
                        'pk': squad_entry['pk'],
                        'sk': f'LINEUP#{CURRENT_YEAR}#' + str(number)
                    },
                    UpdateExpression="set nrl_club=:c",
                    ExpressionAttributeValues={
                        ':c': player[0]['nrl_club']
                    }
                )
            # Write update to logs
            table.put_item(
                Item={
                    'pk': f'NEWS',
                    'sk': f'PLAYER#{player_id}',
                    'data': f'ROUND#{CURRENT_YEAR}#{number}',
                    'datetime': str(datetime.now()),
                    'log': message,
                    'player_id': str(player_id),
                    'round_number': number,
                    'player_name': player[0]['player_name'],
                    'nrl_club': player[0]['nrl_club'],
                    'year': CURRENT_YEAR
                }
            )

        else: 
            squad_entry = squad_entry[0]
            player_id = squad_entry['player_id']

        player[0]['player_id'] = player_id
        player_scores = {}
        player_scores[squad_entry['position']] = {
            'tries': player[1]['Tries'],
            'sin_bins': player[1]['Sin Bins'],
            'send_offs': player[1]['Send Offs'],
            'involvement_try': involvement_try(player[1], squad_entry['position']),
            'positional_try': positional_try(player[1], squad_entry['position']) > 0,
            'mia': missing(player[1], squad_entry['position']),
            'concede': False if positional_try(player[1], squad_entry['position']) > 1 else player[1]['Missed Tackles'] > 4 or player[1]['Errors'] > 2,
            'field_goals': player[1]['1 Point Field Goals'] - player[1]['2 Point Field Goals'], # Website counts 2pt fgs twice so need to get difference
            '2point_field_goals': player[1]['2 Point Field Goals']
        }
        if 'position2' in squad_entry.keys() and squad_entry['position2'] != '' and squad_entry['position2'] != None:
            player_scores[squad_entry['position2']] = {
                'tries': player[1]['Tries'],
                'sin_bins': player[1]['Sin Bins'],
                'send_offs': player[1]['Send Offs'],
                'involvement_try': involvement_try(player[1], squad_entry['position2']),
                'positional_try': positional_try(player[1], squad_entry['position2']) > 0,
                'mia': missing(player[1], squad_entry['position2']),
                'concede': False if positional_try(player[1], squad_entry['position2']) > 1 else player[1]['Missed Tackles'] > 4 or player[1]['Errors'] > 2,
                'field_goals': player[1]['1 Point Field Goals'] - player[1]['2 Point Field Goals'], # Website counts 2pt fgs twice so need to get difference
                '2point_field_goals': player[1]['2 Point Field Goals']
            }
        if 'position3' in squad_entry.keys() and squad_entry['position3'] != '' and squad_entry['position3'] != None:
            player_scores[squad_entry['position3']] = {
                'tries': player[1]['Tries'],
                'sin_bins': player[1]['Sin Bins'],
                'send_offs': player[1]['Send Offs'],
                'involvement_try': involvement_try(player[1], squad_entry['position3']),
                'positional_try': positional_try(player[1], squad_entry['position3']) > 0,
                'mia': missing(player[1], squad_entry['position3']),
                'concede': False if positional_try(player[1], squad_entry['position3']) > 1 else player[1]['Missed Tackles'] > 4 or player[1]['Errors'] > 2,
                'field_goals': player[1]['1 Point Field Goals'] - player[1]['2 Point Field Goals'], # Website counts 2pt fgs twice so need to get difference
                '2point_field_goals': player[1]['2 Point Field Goals']
            }
        player_scores['kicker'] = {
            'goals': player[1]['Conversions'] + player[1]['Penalty Goals']
        }
        player.append(player_scores)

    print("Loading to dynamodb, table: stats2020")
    
    
    for player in player_stats_final:
        
        table.put_item(Item={
            "pk": 'PLAYER#' + player[0]['player_id'],
            "sk": f'STATS#{CURRENT_YEAR}#' + str(number),
            'data': 'CLUB#' + player[0]['nrl_club'],
            "player_id": player[0]['player_id'],
            "round_number": number,
            "player_name": player[0]['player_name'],
            "nrl_club": player[0]['nrl_club'],
            "opponent": player[0]['opponent'],
            "stats": player[1],
            "scoring_stats": player[2],
            'year': CURRENT_YEAR
        })            

    print("Stats update complete, scoring lineups")
    
    fixtures = table.query(
        KeyConditionExpression=Key('pk').eq(f'ROUND#{CURRENT_YEAR}#' + str(number)) & Key('sk').begins_with('FIXTURE')
    )['Items']
    round_lineups = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq(f'LINEUP#{CURRENT_YEAR}#' + str(number)) & Key('data').begins_with('TEAM#')
    )['Items']
    for match in fixtures:
        print(f"Match: {match['home']} v {match['away']}")
        scores = {}
        for team in ['home', 'away']:
            lineup = [p for p in round_lineups if p['xrl_team'] == match[team]]
            print(f"{match[team]} fielded {len(lineup)} players")
            # print("First player: " + str(lineup[0]))
            lineup_score = 0
            for player in lineup:
                player_lineup_score = 0
                playing_score = 0
                kicking_score = 0
                played_nrl = False
                played_xrl = False
                for player_stats in player_stats_final:
                    if player['player_id'] == player_stats[0]['player_id']:
                        played_nrl = player_stats[1]['Mins Played'] > 0
                        played_xrl = played_nrl and not player['position_specific'].startswith('int')
                        if player['position_general'] not in player_stats[2].keys():
                            print(str(player))
                            print(str(player_stats))                       
                        player_scoring_stats = player_stats[2][player['position_general']]
                        player_lineup_score += player_scoring_stats['tries'] * 4
                        player_lineup_score -= player_scoring_stats['sin_bins'] * 2
                        if player_scoring_stats['send_offs'] != 0:
                            minutes = 80 - player_scoring_stats['send_offs']
                            deduction = math.floor(minutes / 10) + 4
                            player_lineup_score -= deduction
                        if player_scoring_stats['involvement_try']: player_lineup_score += 4
                        if player_scoring_stats['positional_try'] > 0: player_lineup_score += 4
                        if player_scoring_stats['mia']: player_lineup_score -= 4
                        if player_scoring_stats['concede']: player_lineup_score -= 4
                        player_lineup_score += player_scoring_stats['field_goals']
                        player_lineup_score += player_scoring_stats['2point_field_goals'] * 2
                        playing_score = player_lineup_score
                        if player['captain'] or player['captain2']:
                            player_lineup_score *= 2
                        player_kicking_stats = player_stats[2]['kicker']
                        kicking_score = player_kicking_stats['goals'] * 2
                        if player['kicker']:
                            player_lineup_score += kicking_score
                
                table.update_item(
                    Key={
                        'pk': player['pk'],
                        'sk': player['sk']
                    },
                    UpdateExpression="set played_nrl=:p, played_xrl=:x, score=:s, playing_score=:ps, kicking_score=:ks",
                    ExpressionAttributeValues={
                        ':p': played_nrl,
                        ':x': played_xrl,
                        ':s': player_lineup_score,
                        ':ps': playing_score,
                        ':ks': kicking_score
                    }
                )
                if played_xrl:
                    lineup_score += player_lineup_score
            scores[team] = lineup_score
        match['home_score'] = scores['home']
        match['away_score'] = scores['away']
        table.update_item(
            Key={
                'pk': match['pk'],
                'sk': match['sk']
            },
            UpdateExpression='set home_score=:hs, away_score=:as',
            ExpressionAttributeValues={
                ':hs': match['home_score'],
                ':as': match['away_score']
            }
        )

    print('Lineup scoring complete')
    finish = datetime.now()
    print(f"Execution took {finish - start}") 
    return f"Scraping complete. Captured {len(player_stats_final)} appearances in {finish - start}."

if __name__ == '__main__':
    get_stats()