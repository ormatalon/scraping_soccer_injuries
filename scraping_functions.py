"""
This module contains all functions required to scrape leagues, teams and
players info out of the top N leagues. Afterwards, save them to JSON files
that are later used to create the initial DB which will be used in throughout
this project.
"""

import requests
from bs4 import BeautifulSoup
import json
import config
from Soccerway_create_db import define_logger

SOCCER_URL = config.SOCCER_URL
logger = define_logger(config.LOGGER, config.LOG_FILE)

def scrape_leagues(top_leagues_num=config.TOP_LEAGUES_NUM):
    """
    create the leagues table.
    Leagues table contain league_name, country and league_url.
    :param top_leagues_num: Number of top leagues to be extracted.
    :return leagues: A dictionary of leagues scraped (also saved as a json file).
    """
    comp_url = SOCCER_URL + "/competitions/"

    competitions_soup = BeautifulSoup(requests.get(comp_url).text, 'html.parser')

    leagues = {}
    counter = 0

    # Run over competitions
    for competition in competitions_soup.find_all('li', class_=["odd", "even"]):
        leagues[counter] = {}
        leagues[counter]['name'] = str(competition.text)
        leagues[counter]['country'] = competition.contents[0].attrs['href'].rsplit('national/')[1].split('/')[0].title()
        leagues[counter]['url'] = SOCCER_URL + competition.contents[0].attrs['href']

        logger.info("\nleague name-" + leagues[counter]['name'] + "\nleague url-" + leagues[counter]['url'])

        counter += 1

        if counter == top_leagues_num:
            break

    return leagues


def scrape_teams(leagues_dict, counter=0):
    """
    create the teams table.
    teams table contain team_name, team_url and league_url (as it's unique).
    :param leagues_dict: a dictionary of leagues to scrape teams from.
    :param counter: Counter of teams in DB (default is 0 for initial creation of JSON files)
    :return teams: A dictionary of teams scraped (also saved as a json file).
    """
    # Consider allowing entering a specific league_url, then scraping teams from this league only

    teams = {}

    for i in leagues_dict.keys():

        league_soup = BeautifulSoup(requests.get(leagues_dict[i]['url']).text, 'html.parser')

        league_table = league_soup.find('table', class_="leaguetable sortable table detailed-table")
        teams_to_scrape = league_table.find_all('td', class_="text team large-link")

        logger.info(f"Links for teams of {leagues_dict[i]['name']}:")

        # Run over teams in a competition
        for team in teams_to_scrape:
            teams[counter] = {}
            teams[counter]['league_id'] = i
            teams[counter]['name'] = team.a["title"]
            teams[counter]['url'] = SOCCER_URL + team.a["href"]

            logger.info(teams[counter]['name'] + ': ' + teams[counter]['url'])

            counter += 1

    return teams


def scrape_players(teams_dict, player_counter=0, injury_counter=0,
                   player_season_counter=0, player_team_counter=0):
    """
    Scrape data regarding players, while calling functions that creates tables
    of their personal info, injuries info and career info (teams they played for
    and seasons they have participated in).
    :param teams_dict: a dictionary of teams to scrape players from.
    :param player_counter: Counter of players in DB (default is 0 for initial creation of JSON files)
    :param injury_counter: Counter of injuries in DB (default is 0 for initial creation of JSON files)
    :param player_season_counter: Counter of players' seasons in DB (default is 0 for initial creation of JSON files)
    :param player_team_counter: Counter of players' teams in DB (default is 0 for initial creation of JSON files)
    :return players: A dictionary of players' personal info scraped (also saved as a json file).
    :return injuries: A dictionary of players' injuries info scraped (also saved as a json file).
    :return players_seasons: A dictionary of players' seasons participated in scraped (also saved as a json file).
    :return players_teams: A dictionary of players' teams scraped (also saved as a json file).
    """
    # Consider allowing entering a specific team_url, then scraping players from this team only

    players = {}
    injuries = {}
    players_seasons = {}
    players_teams = {}

    for i in teams_dict.keys():

        logger.info(f"\nScraping data of players in {teams_dict[i]['name']}.")

        squad_url = teams_dict[i]['url'] + 'squad/'
        squad_soup = BeautifulSoup(requests.get(squad_url).text, 'html.parser')
        squad_table = squad_soup.find('table', class_="table squad sortable")

        if squad_table:
            players_to_scrape = squad_table.find_all('td', class_="name large-link")

            for player_in_squad in players_to_scrape:

                injuries, injury_counter, players_seasons, player_season_counter, \
                players_teams, player_team_counter, players = \
                scrape_specific_player(player_in_squad,
                                       injuries, injury_counter,
                                       players_seasons, player_season_counter,
                                       players_teams, player_team_counter,
                                       players, player_counter, teams_dict)
                logger.info(f"scraped data on player: {player_in_squad.a.text}")
                player_counter += 1

    return players, injuries, players_seasons, players_teams


####################################################


def scrape_specific_player(player, injuries_d, injury_count,
                           players_seasons_d, player_season_count,
                           players_teams_d, player_team_count,
                           players_d, player_count, teams_dictionary):
    """
    :param player: Specific player's soup (parsed HTML)
    :param injuries_d: Current dictionary of injuries
    :param injury_count: Current index (main key) in dictionary of injuries
    :param players_seasons_d: Current dictionary of players' seasons
    :param player_season_count: Current index (main key) in dictionary of players' seasons
    :param players_teams_d: Current dictionary of players' teams
    :param player_team_count: Current index (main key) in dictionary of players' teams
    :param players_d: Current dictionary of players' info
    :param player_count: Current player index, and the main key in dictionary of players' info
    :param teams_dictionary: dictionary of all teams scraped
    :return: All input parameters updated (besides 'player_count').
    """
    player_url = SOCCER_URL + player.a["href"]
    player_soup = BeautifulSoup(requests.get(player_url).text, 'html.parser')

    injuries_d, injury_count = \
        get_player_injuries(player_soup, injuries_d, player_count, injury_count)

    teams_url = [val["url"] for val in teams_dictionary.values()]

    players_seasons_d, player_season_count, first_date, cur_team, cur_team_url = \
        get_player_seasons(player_soup, players_seasons_d, player_count, player_season_count, teams_url)

    players_teams_d, player_team_count = \
        get_player_teams(player_soup, players_teams_d, player_count, player_team_count,
                         teams_url, first_date, cur_team, cur_team_url)

    players_d = get_player_info(player_soup, players_d, player_count, player_url)

    return injuries_d, injury_count, players_seasons_d, player_season_count, \
           players_teams_d, player_team_count, players_d


def get_player_injuries(player_html, injuries_dict, player_num, injury_num):
    """
    Gets the player's injury data from the player's html.
    :param player_html: Specific player's soup (parsed HTML)
    :param injuries_dict: Current dictionary of injuries
    :param player_num: Current player index (and the main key in dictionary of players' info)
    :param injury_num: Current index (main key) in dictionary of injuries
    :return:  Updated injuries_dict and injury_num.
    """
    sideline_table = player_html.find('table', class_="sidelined table")

    if sideline_table:
        injuries_to_scrape = sideline_table.find_all('tr', class_="odd")

        for injury in injuries_to_scrape:
            # if injury.td["title"] != 'Suspended':
            injuries_dict[injury_num] = {}

            injuries_dict[injury_num]['player_id'] = player_num
            injuries_dict[injury_num]['description'] = injury.td["title"]
            injuries_dict[injury_num]['start_date'] = injury.find('td', class_="startdate").string
            injuries_dict[injury_num]['end_date'] = injury.find('td', class_="enddate").string

            injury_num += 1

    return injuries_dict, injury_num


def get_player_info(player_html, players_dict, player_num, player_site):
    """
    Creates the players table.
    players table contain first_name, last_name, nationality, birthplace, birth date,
    position, height, weight, foot_right (boolean) url and team_url (as it's unique).
    :param player_html: Specific player's soup (parsed HTML)
    :param players_dict: Current dictionary of players' info
    :param player_num: Current player index (and the main key in dictionary of players' info)
    :param player_site: Specific player's url in soccerway.com
    :return:  Updated players_dict.
    """
    passport = player_html.find('div', class_="block_player_passport real-content clearfix")

    if passport:
        players_dict[player_num] = {}

        details = passport.find_all('dt')
        results = passport.find_all('dd')

        # Looping over player's personal details
        for i in range(len(details)):
            players_dict[player_num][details[i].text] = results[i].text

        players_dict[player_num]['url'] = player_site

        if "Date of birth" not in players_dict[player_num].keys():
            players_dict[player_num]["Date of birth"] = 'nan'
        if "Position" not in players_dict[player_num].keys():
            players_dict[player_num]["Position"] = "Unknown"
        if "Country of birth" not in players_dict[player_num].keys():
            players_dict[player_num]["Country of birth"] = "Unknown"
        if "Height" not in players_dict[player_num].keys():
            players_dict[player_num]["Height"] = None
        if "Weight" not in players_dict[player_num].keys():
            players_dict[player_num]["Weight"] = None
        if "Foot" not in players_dict[player_num].keys():
            players_dict[player_num]["Foot"] = None

    return players_dict


def get_player_seasons(player_html, player_seasons_dict, player_num, player_season_num, teams_urls):
    """
    Gets the player's seasons data from the player html.
    :param player_html: Specific player's soup (parsed HTML)
    :param player_seasons_dict: Current dictionary of players' seasons info
    :param player_num: Current player index (and the main key in dictionary of players' info)
    :param player_season_num: Current index (main key) in dictionary of players' seasons
    :param teams_urls: List of URLs of all scraped teams
    :return player_first_date: First date a player have played for a team
    :return current_team: Player's current team
    :return current_team_url: URL of the player's current team
    :return: Updated player_seasons_dict and player_season_num
    """
    career_table = player_html.find('table', class_="playerstats career sortable table")

    if career_table:
        details = get_career_table_headers(career_table)    # Prepare the table's columns names ("details")
        player_seasons_to_scrape = career_table.tbody.find_all('tr')    # Find all seasons player have played at

        # Run over all player's seasons (/ part seasons)
        for season_ind, player_season in enumerate(player_seasons_to_scrape):
            player_seasons_dict[player_season_num] = {}
            player_seasons_dict[player_season_num]['player_id'] = player_num

            results = player_season.find_all('td')
            # Run all over player's stats during this season (/ part season)
            for i in range(len(details)):
                if details[i] in ['Comp', 'Team']:    # For league and team - add another URL column
                    player_seasons_dict[player_season_num][details[i]] = results[i].a["title"]
                    player_seasons_dict[player_season_num][details[i]+'_url'] = SOCCER_URL + results[i].a["href"]

                    # For teams only - save their team_id too, if exist in "teams" dictionary
                    if player_seasons_dict[player_season_num][details[i]+'_url'] in teams_urls:
                        player_seasons_dict[player_season_num][details[i]+'_id'] = teams_urls.index(
                            player_seasons_dict[player_season_num][details[i]+'_url'])
                    elif details[i] == 'Team':
                        player_seasons_dict[player_season_num][details[i] + '_id'] = None
                    continue

                player_seasons_dict[player_season_num][details[i]] = results[i].text

            if season_ind == 0:
                current_team = player_seasons_dict[player_season_num]["Team"]
                current_team_url = player_seasons_dict[player_season_num]["Team_url"]

            player_season_num += 1

        # The first date in which a player assumed to be in any team is
        # January 1st of his first season (which is ordered last in the seasons table)
        player_first_date = '01/01/' + player_seasons_dict[player_season_num-1][details[0]].split('/')[0][2:]

    else:
        player_first_date, current_team, current_team_url = None, None, None

    return player_seasons_dict, player_season_num, player_first_date, current_team, current_team_url


def get_player_teams(player_html, player_teams_dict, player_num, player_team_num, teams_urls,
                     initial_date, team_now, team_now_url):
    """
    Gets the player's seasons data from the player html
    :param player_html: Specific player's soup (parsed HTML)
    :param player_teams_dict: Current dictionary of players' teams info
    :param player_num: Current player index (and the main key in dictionary of players' info)
    :param player_team_num: Current index (main key) in dictionary of players' teams
    :param teams_urls: List of URLs of all scraped teams
    :param initial_date: First date a player have played for a team
    :param team_now: Player's current team
    :param team_now_url: URL of the player's current team
    :return: Updated player_teams_dict and player_team_num
    """
    transfers_table = player_html.find('table', class_="transfers table")
    DATE_IND, FROM_IND, TO_IND = (0, 1, 2)  # Useful results indices meaning

    if transfers_table:
        player_transfers_to_scrape = transfers_table.find_all('tr', class_=["odd", "even"])    # Find all player's transfers

        # Run over all player's transfers
        for transfer_ind, player_transfer in enumerate(player_transfers_to_scrape):
            results = player_transfer.find_all('td')

            end = None if transfer_ind == 0 else player_teams_dict[player_team_num - 1]["start"]
            player_teams_dict, player_team_num = create_player_team_row(player_teams_dict,
                                                 player_team_num, player_num,
                                                 results[TO_IND].a["title"],
                                                 SOCCER_URL + results[TO_IND].a["href"],
                                                 teams_urls, results[DATE_IND].text, end)

        # Add First team data:
        player_teams_dict, player_team_num = create_player_team_row(player_teams_dict,
                                                   player_team_num, player_num,
                                                   results[FROM_IND].a["title"],
                                                   SOCCER_URL + results[FROM_IND].a["href"],
                                                   teams_urls, initial_date,
                                                   player_teams_dict[player_team_num - 1]["start"])

    # If player wasn't transferred at all (one team only)
    else:
        player_teams_dict, player_team_num = create_player_team_row(player_teams_dict,
                                                   player_team_num, player_num,
                                                   team_now, team_now_url,
                                                   teams_urls, initial_date, None)

    return player_teams_dict, player_team_num


####################################################


def get_career_table_headers(career_table):
    """
    Creates a list of the career table headers
    :param career_table: html of career table.
    :return details: List of career table's headers.
    """
    LAST_HEADER_WITH_TEXT = 2   # Consider moving to .cnf file

    details = []  # Career table's heads of columns, used only once
    for i in range(len(career_table.thead.tr.find_all('th'))):

        if i <= LAST_HEADER_WITH_TEXT:
            details.append(career_table.thead.tr.find_all('th')[i].text)
        else:
            details.append(career_table.thead.tr.find_all('th')[i].contents[0].attrs["title"])

    return details


def create_player_team_row(player_teams_d, player_team_n, player_n, team_name,
                           team_url, teams_url_list, start, end):
    """
    Add a row of the player's team table into the player_teams dictionary
    :param player_teams_d: Current dictionary of players' teams info
    :param player_team_n: Current index (main key) in dictionary of players' teams
    :param player_n: Current player index (and the main key in dictionary of players' info)
    :param team_name: Name of team to be added
    :param team_url: URL of team to be added
    :param teams_url_list: List of URLs of all scraped teams
    :param start: Starting date in the team
    :param end: Last date in the team
    :return: Updated player_teams_d and player_team_n
    """
    player_teams_d[player_team_n] = {}
    player_teams_d[player_team_n]['player_id'] = player_n

    player_teams_d[player_team_n]["team"] = team_name
    player_teams_d[player_team_n]["team_url"] = team_url
    if player_teams_d[player_team_n]["team_url"] in teams_url_list:
        player_teams_d[player_team_n]["team_id"] = teams_url_list.index(
            player_teams_d[player_team_n]["team_url"])
    else:
        player_teams_d[player_team_n]["team_id"] = None

    player_teams_d[player_team_n]["start"] = start
    player_teams_d[player_team_n]["end"] = end

    player_team_n += 1

    return player_teams_d, player_team_n


####################################################


def save_json_leagues(leagues):
    with open("league.json", 'w') as json_file:
        json_file.write(json.dumps(leagues))


def save_json_teams(teams):
    with open("team.json", 'w') as json_file:
        json_file.write(json.dumps(teams))


def save_json_players(players):
    with open("player.json", 'w') as json_file:
        json_file.write(json.dumps(players))


def save_json_injuries(injuries):
    with open("injury.json", 'w') as json_file:
        json_file.write(json.dumps(injuries))


def save_json_players_seasons(players_seasons):
    with open("player_season.json", 'w') as json_file:
        json_file.write(json.dumps(players_seasons))


def save_json_players_teams(players_teams):
    with open("player_team.json", 'w') as json_file:
        json_file.write(json.dumps(players_teams))


####################################################


def main():
    """
    Activate the functions scraping the info of N top leagues from soccerway.com
    :return: -
    """
    leagues = scrape_leagues()
    save_json_leagues(leagues)

    teams = scrape_teams(leagues)
    save_json_teams(teams)

    players, injuries, players_seasons, players_teams = scrape_players(teams)
    if players:
        save_json_players(players)
        save_json_injuries(injuries)
        save_json_players_seasons(players_seasons)
        save_json_players_teams(players_teams)


if __name__ == '__main__':
    main()
