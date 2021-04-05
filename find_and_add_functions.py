
import requests
from bs4 import BeautifulSoup
import pymysql
import click
import datetime

import config
from scraping_functions import scrape_leagues, scrape_teams, scrape_players
from Soccerway_create_db import define_logger, create_teams, create_players,\
    create_injuries, create_players_by_team, create_players_by_season


SOCCER_URL = config.SOCCER_URL
DB_NAME = config.DB_NAME
logger = define_logger(config.LOGGER, config.LOG_FILE)


def find_league(argin, argtype, host='localhost', root='root', password=''):
    """
    Find league in current DB. If it doesn't exist - add it by using add_league function.
    :param argin: Input arguments to be found.
    :param argtype: Type of inputs to be found (name/ country/ url).
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return results: Query's results
    """
    con = pymysql.connect(host, root, password)
    results = []

    with con.cursor() as cur:
        cur.execute(f"""USE {DB_NAME};""")

        for inp in argin:
            if argtype == "country" and inp not in get_countries_dict().keys():
                logger.info(f"{inp} is not one of the possible countries.")
                continue

            cur.execute(f"""SELECT * FROM leagues WHERE {argtype}='{inp}'""")
            result = cur.fetchall()
            if result:
                results.append(result)

            else:
                logger.info(f"League is not inside the DB - starting to scrape its teams and players.")
                add_league(inp, argtype, con, host, root, password)
                cur.execute(f"""SELECT * FROM leagues WHERE {argtype}='{inp}'""")
                result = cur.fetchall()
                if result:
                    results.append(result)
                else:
                    logger.info(f"{inp} was not found on site.")

    con.close()
    return results


def add_league(inp_to_add, type_to_add, con, host, root, password):
    """
    Add a league which doesn't exist in the DB to it.
    :param inp_to_add: Input to be added.
    :param type_to_add: Type of input to ba added.
    :param con: DB connection
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return:
    """
    with con.cursor() as cur:
        if type_to_add == "url":
            league_soup = BeautifulSoup(requests.get(inp_to_add).text, 'html.parser')
            league_site = inp_to_add
        elif type_to_add == "country":
            midterm_url = get_countries_dict()[inp_to_add]
            league_soup = BeautifulSoup(requests.get(midterm_url).text, 'html.parser')
            league_site = SOCCER_URL + league_soup.find('ul', class_="left-tree").li.a["href"]
        else:
            league_soup, league_site = get_first_search_result(
                SOCCER_URL + "/search/competitions/?q=" + inp_to_add)

        if league_soup:
            cur.execute("SELECT MAX(id) FROM leagues")
            league_id = cur.fetchall()[0][0]

            addition = (league_soup.body.h1.text, league_soup.body.h2.text, league_site)
            cur.execute("""INSERT INTO leagues (name, country, url) VALUES (%s, %s, %s)""", addition)
            con.commit()

            league_dict = {league_id: {'name': addition[0], 'url': addition[2]}}
            add_all_teams_and_players_in_league(league_dict, con, host, root, password)


def get_first_search_result(searching_site, player=None):
    """
    Get the HTML of the first relevant result in soccerway.com
    :param searching_site: URL of the search's results in soccerway.com
    :param player: Flag which indicates if it is a player being searched.
    :return result_soup: HTML of the first result's site
    :return result_site: URL of the first result's site
    """
    search_soup = BeautifulSoup(requests.get(searching_site).text, 'html.parser')

    first_result = None
    result_soup = None
    result_site = None

    if player:
        search_result = search_soup.find('table', class_="playerstats table")
        if search_result:
            first_result = search_soup.find('table', class_="playerstats table").\
                find('td', class_="player").a["href"]
    else:
        search_result = search_soup.find('ul', class_="tree search-results")
        if search_result:
            first_result = search_soup.find('ul', class_="tree search-results").li.a.attrs['href']

    if first_result:
        result_site = SOCCER_URL + first_result
        result_soup = BeautifulSoup(requests.get(result_site).text, 'html.parser')

    return result_soup, result_site


def get_countries_dict():
    """
    Get the countries that exist in soccerway.com and the midterm URL to their first leagues
    :return countries_dict: Dictionary of countries exist in soccerway.com and their URL
    """
    competitions_page_soup = BeautifulSoup(requests.get(
        SOCCER_URL + "/competitions/").text, 'html.parser')

    competitions_page_soup = competitions_page_soup.find('ul', class_='areas')
    countries_soup = competitions_page_soup.find_all('div', class_="row")

    countries_dict = {}
    for country_soup in countries_soup:
        countries_dict[country_soup.a.text.strip().title()] = SOCCER_URL + country_soup.a["href"]

    return countries_dict


def add_all_teams_and_players_in_league(league_dict, con, host, root, password):
    """
    Add all teams and players data within the league to be added
    :param league_dict:
    :param con:
    :param host:
    :param root:
    :param password:
    :return:
    """
    with con.cursor() as cur:
        cur.execute("""SELECT MAX(id) FROM teams""")
        team_counter = cur.fetchall()[0][0]

        cur.execute("""SELECT MAX(id) FROM players""")
        player_count = cur.fetchall()[0][0]

        cur.execute("""SELECT MAX(id) FROM injuries""")
        injury_count = cur.fetchall()[0][0]

        cur.execute("""SELECT MAX(id) FROM player_season""")
        player_season_count = cur.fetchall()[0][0]

        cur.execute("""SELECT MAX(id) FROM player_team""")
        player_team_count = cur.fetchall()[0][0]

        teams_dict = add_league_teams(league_dict, team_counter, host, root, password)

        add_teams_players(teams_dict, player_count, injury_count, player_season_count,
                          player_team_count, host, root, password)


def add_league_teams(league_diction, team_count, host, root, password):
    """
    Add all teams in a league which doesn't exist in the DB to it.
    :param league_diction: Dictionary of league to add, with its name and url.
    :param team_count: Number of leagues currently in the DB
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return teams_diction: Dictionary with the teams in the new league added to the DB
    """
    teams_diction = scrape_teams(league_diction, team_count)
    create_teams(host, root, password, dict_to_read=teams_diction)

    return teams_diction


def add_teams_players(teams_dictionary, player_counter, injury_counter, player_season_counter,
                      player_team_counter, host, root, password):
    """
    Add all teams in a league which doesn't exist in the DB to it.
    :param teams_dictionary: Dictionary of teams to add.
    :param player_counter: Number of players currently in the DB
    :param injury_counter: Number of injuries currently in the DB
    :param player_season_counter: Number of players' seasons currently in the DB
    :param player_team_counter: Number of players' teams currently in the DB
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return :
    """
    players, injuries, players_seasons, players_teams = \
        scrape_players(teams_dictionary, player_counter, injury_counter, player_season_counter,
                       player_team_counter)
    if players:
        create_players(host, root, password, dict_to_read=players)
        create_injuries(host, root, password, dict_to_read=injuries)
        create_players_by_team(host, root, password, dict_to_read=players_teams)
        create_players_by_season(host, root, password, dict_to_read=players_seasons)


################


def find_team(argin, argtype, host='localhost', root='root', password=''):
    """
    Find team in current DB. If it doesn't exist - add it by using add_league function.
    :param argin: Input arguments to be found.
    :param argtype: Type of inputs to be found (name).
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return results: Query's results
    """
    con = pymysql.connect(host, root, password)
    results = []

    with con.cursor() as cur:
        cur.execute(f"""USE {DB_NAME};""")

        for inp in argin:
            cur.execute(f"""SELECT * FROM teams WHERE {argtype}='{inp}'""")
            result = cur.fetchall()
            if result:
                results.append(result)

            else:
                team_name = add_team(inp, argtype, host, root, password)
                con.commit()
                cur.execute(f"""SELECT * FROM teams WHERE name='{team_name}'""")
                result = cur.fetchall()
                if result:
                    results.append(result)
                else:
                    logger.info(f"{inp} was not found on site.")

    con.close()
    return results


def add_team(inp_to_add, type_to_add, host, root, password):
    """
    Add a team which doesn't exist in the DB to it.
    :param inp_to_add: Input to be added.
    :param type_to_add: Type of input to ba added.
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return:
    """
    team_name = ""

    if type_to_add == "url":
        team_soup = BeautifulSoup(requests.get(inp_to_add).text, 'html.parser')
        team_site = inp_to_add
    else:
        team_soup, team_site = get_first_search_result(
            SOCCER_URL + "/search/teams/?q=" + inp_to_add)

    if team_soup:
        # Need to examine if league already exists, if not - add it. Then, get its LEAGUE_ID
        league_url = SOCCER_URL + team_soup.find('div', id="page_team_1_block_team_table_9-wrapper").h2.a["href"]
        find_league({league_url}, "url", host, root, password)

        team_name = team_soup.find("table", class_="leaguetable sortable table").tbody.find_all(
            'tr', class_=["odd highlight team_rank", "even highlight team_rank"])[0].find(
            'td', class_="text team large-link").a.text

    return team_name


################


def find_player(argin, argtype, host='localhost', root='root', password=''):
    """
    Find player in current DB. If it doesn't exist - add it by using add_league function.
    :param argin: Input arguments to be found.
    :param argtype: Type of inputs to be found (name).
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return results: Query's results
    """
    con = pymysql.connect(host, root, password)
    results = []

    with con.cursor() as cur:
        cur.execute(f"""USE {DB_NAME};""")

        for inp in argin:
            if argtype == "name":
                if len(inp.split()) == 1:
                    cur.execute(f"""SELECT * FROM players WHERE last_name LIKE '%{inp}%'""")
                else:
                    first_name = inp.split()[0]
                    last_name = inp.split()[-1]
                    cur.execute(f"""SELECT * FROM players WHERE
                                    first_name LIKE '%{first_name}%' AND
                                    last_name LIKE '%{last_name}%'""")
            else:
                cur.execute(f"""SELECT * FROM players WHERE {argtype}='{inp}'""")

            result = cur.fetchall()
            if result:
                results.append(result)

            else:
                first, last = add_player(inp, argtype, host, root, password)
                con.commit()
                cur.execute(f"""SELECT * FROM players WHERE
                                                first_name='{first}' AND
                                                last_name='{last}'""")
                result = cur.fetchall()
                if result:
                    results.append(result)
                else:
                    logger.info(f"{inp} was not found on site.")

    con.close()
    return results


def add_player(inp_to_add, type_to_add, host, root, password):
    """
    Add a player which doesn't exist in the DB to it.
    :param inp_to_add: Input to be added.
    :param type_to_add: Type of input to ba added.
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return detail_dict["First name"]: Player's first name
    :return detail_dict["Last name"]: Player's last name
    """
    detail_dict = {}

    if type_to_add == "url":
        player_soup = BeautifulSoup(requests.get(inp_to_add).text, 'html.parser')
        player_site = inp_to_add
    else:
        player_soup, player_site = get_first_search_result(
            SOCCER_URL + "/search/players/?q=" + inp_to_add, player=1)

    if player_soup:

        passport = player_soup.find('div', class_="block_player_passport real-content clearfix")

        if passport:
            details = passport.find_all('dt')
            results = passport.find_all('dd')

            detail_dict = {}
            for i in range(len(details)):
                detail_dict[details[i].text] = results[i].text

        league_url = SOCCER_URL + player_soup.find('table', class_="playerstats career sortable table")\
            .tbody.tr.find('td', class_="competition").a["href"]
        find_league({league_url}, "url", host, root, password)

    return detail_dict["First name"], detail_dict["Last name"]    # Return first and last name as in DB


########################################################


@click.command()
@click.option('--password', '-ps', default='', prompt='Please insert the MySQL password:', help='MySQL password')
def main(password):
    """
    :param password: MySQL password
    :return:
    """
    print(find_team({"Olympiakos Nicosia"}, "name", password=password))


if __name__ == '__main__':
    main()
