
import pandas as pd
import requests
import datetime
import pymysql
import config
from Soccerway_create_db import define_logger
import sys

logger = define_logger(config.LOGGER, config.LOG_FILE)

# Continent code (Europe):
CONTINENT = 'eu'

# La-Liga tournament code:
TOURNAMENT_DICT = config.API_TOURNAMENT_DICT

# Europe leagues API key:
api_key = 'bb3jjg4dem9u5hzjhajszbuw'


###############################


def check_player_not_in_db(first_name='Lionel', last_name='Messi', password=''):
    con = pymysql.connect(config.HOST, config.ROOT, password, config.DB_NAME)
    res = pd.read_sql(f"""SELECT first_name, last_name 
                    FROM players 
                    WHERE first_name LIKE '%{first_name}%' AND last_name LIKE '%{last_name}%';
                    """, con)
    con.close()
    if res.shape[0] == 1:
        logger.info(f"Player {first_name} {last_name} already exist in the database.")
        return False
    elif res.shape[0] > 1:
        logger.warning(f"More than one player in the database named {first_name} {last_name}.")
        return False
    else:
        logger.info(f"Player {first_name} {last_name} was not in database.")
        return True


###############################


def api_players(players_in_team, password):
    con = pymysql.connect(config.HOST, config.ROOT, password, config.DB_NAME)
    p_counter = pd.read_sql("""SELECT COUNT(*) FROM players;""", con).iloc[0,0] + 1
    con.close()
    players = {}

    for player in players_in_team:
        players[p_counter] = {}

        name = player["name"].split(', ') if "name" in player.keys() else []
        if len(name) > 1:
            players[p_counter]["First name"] = name[1]
            players[p_counter]["Last name"] = name[0]
        elif len(name) == 1:
            players[p_counter]["First name"] = None
            players[p_counter]["Last name"] = name[0]
        else:
            players[p_counter]["First name"] = None
            players[p_counter]["Last name"] = None

        if check_player_not_in_db(first_name=players[p_counter]["First name"],
                                  last_name=players[p_counter]["Last name"], password=password):
            players[p_counter]["Nationality"] = player["nationality"] if "nationality" in player.keys() else None
            players[p_counter]["Date_of_birth"] = player["date_of_birth"] if "date_of_birth" in player.keys() else None
            players[p_counter]["Date_of_birth"] = \
                datetime.datetime.strptime(str(players[p_counter]["Date_of_birth"]), "%Y-%m-%d")
            players[p_counter]["Place_of_birth"] = player["Place of birth"] \
                if "Place of birth" in player.keys() else None
            players[p_counter]["Position"] = player["type"].capitalize() if "type" in player.keys() else None

            players[p_counter]["Height"] = player["height"] if "height" in player.keys() else None
            # added " cm" because our original data from soccerway contains it
            players[p_counter]["Weight"] = player["weight"] if "weight" in player.keys() else None
            # added " kg" because our original data from soccerway contains it

            if "preferred_foot" in player.keys():
                if player["preferred_foot"] == 'right':
                    players[p_counter]["Foot"] = True
                elif player["preferred_foot"] == 'left':
                    players[p_counter]["Foot"] = False
                else:
                    players[p_counter]["Foot"] = None
            else:
                players[p_counter]["Foot"] = None
            players[p_counter]["url"] = None    # This API doesn't work with our soccerway url "system"
            con = pymysql.connect(config.HOST, config.ROOT, password, config.DB_NAME)
            pd.read_sql("SELECT COUNT(*) FROM players", con)
            with con.cursor() as cur:
                cur.execute(f"""INSERT INTO players (first_name, 
                                                last_name,
                                                nationality,
                                                birthdate,
                                                birthplace,
                                                position,
                                                height,
                                                weight,
                                                foot_right,
                                                url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        list(players[p_counter].values()))
                con.commit()
                con.close()
                logger.info(f"The player {players[p_counter]['First name']} \
{players[p_counter]['Last name']} was added to the database.")
            p_counter += 1


###############################


def api_teams(teams_in_league, league_id, password):
    con = pymysql.connect(config.HOST, config.ROOT, password, config.DB_NAME)
    t_counter = pd.read_sql("""SELECT COUNT(*) FROM teams;""", con).iloc[0, 0] + 1
    con.close
    teams = {}

    for team in teams_in_league:
        teams[t_counter] = {}
        teams[t_counter]["league_id"] = league_id
        teams[t_counter]["name"] = team["name"]
        teams[t_counter]["url"] = None   # This API doesn't work with our soccerway url "system"
        team_url = \
            f"https://api.sportradar.us/soccer-t3/{CONTINENT}/en/teams/{team['id']}/profile.json?api_key={api_key}"
        team_data = requests.get(team_url).json()

        logger.info(f"getting players for team {team_data['team']['name']}.")
        api_players(team_data["players"], password)

        t_counter += 1


###############################


def api_league(password, league_code):
    league_url = f"https://api.sportradar.us/soccer-t3/{CONTINENT}/en/tournaments/sr:tournament:\
{league_code}/info.json?api_key={api_key}"
    
    con = pymysql.connect(config.HOST, config.ROOT, password, config.DB_NAME)
    l_counter = pd.read_sql("""SELECT COUNT(*) FROM leagues;""", con).iloc[0, 0] + 1
    con.close()
    league_data = requests.get(league_url).json()

    logger.info(f"getting the league information.")
    leagues = {}

    leagues[l_counter] = {}
    leagues[l_counter]['name'] = league_data["tournament"]["name"]
    leagues[l_counter]['country'] = league_data["tournament"]["category"]["name"]
    leagues[l_counter]['url'] = None   # This API doesn't work with our soccerway url "system"

    api_teams(league_data["groups"][0]["teams"], l_counter, password)


def main():
    password = input('Please insert your MySQL password:')
    print("Please choose the code (number) of one of the following leagues, which will be added to the DB:\n")
    for ind, key in enumerate(config.API_TOURNAMENT_DICT.keys()):
        print(f"{ind}: {key}")

    try:
        tour_code = int(input())
    except (ValueError, TypeError):
        logger.error(f"Input is not valid (integers only between 0 and {len(config.API_TOURNAMENT_DICT-1)}! \
Please run the program again.")
        sys.exit()
        
    if tour_code in list(zip(*enumerate(config.API_TOURNAMENT_DICT.keys())))[0]:
        api_league(password,
                   config.API_TOURNAMENT_DICT[list(zip(*enumerate(config.API_TOURNAMENT_DICT.keys())))[1][tour_code]])
    else:
        logger.error("Input wasn't in the options! Please run the program again.")
        sys.exit()


if __name__ == "__main__":
    main()
