import csv
import json
import math
import os
import stat
import sys
import time
from datetime import datetime
from decimal import Decimal

import boto3
import gspread
from boto3.dynamodb.conditions import Attr, Key
from botocore.errorfactory import ClientError
from google.oauth2.service_account import Credentials
from headless_chrome import create_driver
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.expected_conditions import \
    presence_of_element_located
from selenium.webdriver.support.ui import WebDriverWait

CURRENT_YEAR = 2025

def lambda_handler(event, context):
    start = datetime.now()
    print(f"Script executing at {start}")

    # Initiate database connection
    dynamodbResource = boto3.resource('dynamodb', 'ap-southeast-2')   
    table = dynamodbResource.Table('XRL2021')

    # Get current round
    in_progress_round = table.query(
        IndexName='sk-data-index',
        KeyConditionExpression=Key('sk').eq('STATUS') & Key('data').eq('ACTIVE#true'),
        FilterExpression=Attr('in_progress').eq(True) & Attr('completed').eq(False)
    )['Items'][0]

    in_progress_round_no = in_progress_round['round_number']
    print(f"Scraping stats for Round {in_progress_round_no}")

    # chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--no-sandbox')
    # chrome_options.add_argument('--headless')
    # chrome_options.add_argument('--single-process')
    # chrome_options.add_argument('--disable-dev-shm-usage')
    # chrome_options.binary_location = f"/opt/headless-chromium"
    # driver = webdriver.Chrome(
    #     executable_path = f"/opt/chromedriver",
    #     chrome_options=chrome_options
    # )   

    driver = create_driver()
        
    #draw_url = 'https://www.nrl.com/draw/'
    draw_url = f'https://www.nrl.com/draw/?competition=111&season={CURRENT_YEAR}&round={in_progress_round_no}'
    match_url_base = f'https://www.nrl.com/draw/nrl-premiership/{CURRENT_YEAR}/'

    # Set timeout time
    wait = WebDriverWait(driver, 10)
    # retrive URL in headless browser
    print("Connecting to http://www.nrl.com/draw")
    driver.get(draw_url)

    # round_number = driver.find_element_by_class_name(
    # "filter-round__button filter-round__button--dropdown"
    # ).text
    round_number = driver.find_element_by_css_selector(
        "button[class='filter-round__button filter-round__button--dropdown']"
        ).text
    round_number = round_number.split()
    print(round_number)
    number = round_number[1]
    round_number = "-".join(round_number)

    stat_columns_final = ['Round', 'Team']

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
                ul = div.find_element_by_tag_name('ul')
                lis = ul.find_elements_by_tag_name('li')
                for li in lis:
                    print("Red card: " + li.text)
                    split = li.text.split()
                    name = ' '.join(split[:-1])
                    minute = split[-1][:-1]
                    send_offs[name] = minute

        # find player stats
        try:
            player_stats = driver.find_element_by_link_text("Player Stats")
        except NoSuchElementException:
            print(f"\u001b[31mCouldn't get player stats for {title}\u001b[0m")
            continue
        player_stats.send_keys(Keys.RETURN)

        wait.until(presence_of_element_located((By.ID, "tabs-match-centre-3")))
        # time.sleep(3)
        
        # Find head of table with column names and parse into stat_columns list
        if match_count == 1:
            head = driver.find_element_by_tag_name("thead")
            stats_row = head.find_elements_by_tag_name("th")

            stat_columns = []
            for col in stats_row:
                stat_columns.append(col.text)

            stat_columns = [stat for stat in stat_columns if stat != '']
            stat_columns = stat_columns[10:]
            stat_columns_final += stat_columns
            #print(stat_columns_final)
            #print(len(stat_columns_final))

            # Scrape player stats into list
        home_file = []
        away_file = []

        home_stats = driver.find_elements_by_class_name('table-tbody__tr')  
        for player in home_stats:
            home_file.append(player.text)

        # Press button for away team
        try:
            away_stats_button = f"button[class='toggle-group__item u-flex-center t-{away_team.lower().replace(' ', '-')}']"
            driver.execute_script("arguments[0].click();", driver.find_element_by_css_selector(away_stats_button))
        except NoSuchElementException:
            print(f"\u001b[31mCouldn't get away stats for {title}\u001b[0m")

        # Scrape player stats for away team
        away_stats = driver.find_elements_by_class_name('table-tbody__tr') 
        for player in away_stats:
            away_file.append(player.text)

        home_stats = []
        home_players = []
        away_stats = []
        away_players = []

        # Go through list of rows and append stats (starting with digit) to stats
        # and player names to players, ignoring blank entries
        for row in home_file:
            if len(row) > 0:
                if row[0].isdigit():
                    home_stats.append(row)
                elif row[0].isalpha():
                    home_players.append(row)

        for row in away_file:
            if len(row) > 0:
                if row[0].isdigit():
                    away_stats.append(row)
                elif row[0].isalpha():
                    away_players.append(row)     

        # Create final lists for player stats, converting column info to correct type and format
        home_final = []
        away_final = []

        for i in range(len(home_players)):
            player = []
            player.append(number)
            player.append(home_team)
            player.append(home_players[i])
            ps = home_stats[i].split()
            for j in range(len(ps)):
                if j == 1:
                    player.append(ps[j])
                elif ':' in ps[j]:
                    player.append(int(ps[j][:2]))
                elif '%' in ps[j]:
                    player.append(float(ps[j][:-1]) / 100)
                elif '.' in ps[j]:
                    if ps[j][-1] == 's':
                        player.append(float(ps[j][:-1]))
                    else:
                        player.append(float(ps[j]))
                elif ps[j] == '-':
                    player.append(0)
                elif ps[j] == '2nd':
                    player.append('2nd Row')
                elif ps[j] == 'Row':
                    continue
                else:
                    try:
                        player.append(int(ps[j]))
                    except ValueError:
                        player.append(ps[j])
            
            home_final.append(player)

        for i in range(len(away_players)):
            player = []
            player.append(number)
            player.append(away_team)
            player.append(away_players[i])
            ps = away_stats[i].split()
            for j in range(len(ps)):
                if j == 1:
                    player.append(ps[j])
                elif ':' in ps[j]:
                    player.append(int(ps[j][:2]))
                elif '%' in ps[j]:
                    player.append(float(ps[j][:-1]) / 100)
                elif '.' in ps[j]:
                    if ps[j][-1] == 's':
                        player.append(float(ps[j][:-1]))
                    else:
                        player.append(float(ps[j]))
                elif ps[j] == '-':
                    player.append(0)
                elif ps[j] == '2nd':
                    player.append('2nd Row')
                elif ps[j] == 'Row':
                    continue
                else:
                    try:
                        player.append(int(ps[j]))
                    except ValueError:
                        player.append(ps[j])
            
            away_final.append(player)

        player_stats_final += home_final + away_final

        final_stats = [stat_columns_final] + player_stats_final
    
    # Define variables for connecting to google drive
    print('\u001b[32mOpening google sheet\u001b[0m')
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    info = json.loads(os.environ['XRL_SHEET_CREDENTIALS'])

    credentials = Credentials.from_service_account_info(info, scopes=scope)
    client = gspread.authorize(credentials)

    # Open sheet for round
    spreadsheet = client.open(f'Stats{CURRENT_YEAR}')
    spreadsheet.add_worksheet(round_number, 400, 100)

    spreadsheet.values_update(
        round_number,
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': list(final_stats)}
    )

    finish = datetime.now()
    print(f"Execution took {finish - start}")       
            

