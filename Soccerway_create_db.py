
import pymysql
import pandas as pd
import datetime
import click
import logging
import sys
import config

DB_NAME = config.DB_NAME
LOGGER = config.LOGGER
LOGGER_FILE = config.LOG_FILE


def define_logger(logger_name, log_file):
    """defining logging parameters
    """
    logging.getLogger(logger_name).handlers.clear()
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    ##
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    ##
    logger.addHandler(file_handler)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    return logger


def create_db(host, root, password):
    """
    connect to mysql and create a new schema.
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return: 
    """
    con = pymysql.connect(host, root, password)
    with con.cursor() as cur:
        cur.execute(f"""CREATE DATABASE IF NOT EXISTS {DB_NAME};""")
        cur.execute(f"""USE {DB_NAME};""")
    con.commit()
    con.close()
    logger.warning(f"Database {DB_NAME} already exists.")
    logger.info(f"The database {DB_NAME} exists.")


def create_leagues(host, root, password, dict_to_read=None):
    """
    create the leagues table in the database.
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :param dict_to_read: Dictionary to read instead of a json file, if it is not the initial creation of DB
    :return:
    """
    con = pymysql.connect(host, root, password, db=DB_NAME)
    with con.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS leagues (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(50) CHARACTER SET utf8mb4,
                    country VARCHAR(25) CHARACTER SET utf8mb4,
                    url VARCHAR(255))''')

        # Reading the default json file if no other dictionary was sent as an argument
        if dict_to_read:
            dict_df = pd.DataFrame(dict_to_read).T
        else:
            dict_df = pd.read_json("league.json").T

        for ROW in dict_df.index:
            cur.execute("""INSERT INTO leagues (name, country, url) VALUES (%s, %s, %s)""",
                        list(dict_df.loc[ROW, :]))
    con.commit()
    logger.warning(f"leagues table already exists.")
    logger.info("leagues table exists.")


def create_teams(host, root, password, dict_to_read=None):
    """
    create the teams table in the database.
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :param dict_to_read: Dictionary to read instead of a json file, if it is not the initial creation of DB
    :return:
    """
    con = pymysql.connect(host, root, password, db=DB_NAME)
    with con.cursor() as cur:
        cur.execute(f"""USE {DB_NAME};""")

        cur.execute('''CREATE TABLE IF NOT EXISTS teams (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            league_id INT,
                            name VARCHAR(50) CHARACTER SET utf8mb4,
                            url VARCHAR(255),
                            FOREIGN KEY (league_id) 
                            REFERENCES leagues(id))''')

        # Reading the default json file if no other dictionary was sent as an argument
        if dict_to_read:
            dict_df = pd.DataFrame(dict_to_read).T
        else:
            dict_df = pd.read_json("team.json").T

        dict_df["league_id"] += 1

        logger.info(f"Starts updating teams table.")
        for ROW in dict_df.index:
            cur.execute("""INSERT INTO teams (league_id, name, url) VALUES (%s, %s, %s)""",
                        list(dict_df.loc[ROW, :]))

    con.commit()
    logger.warning(f"teams table already exists.")
    logger.info("teams table exists.")


def create_players(host, root, password, dict_to_read=None):
    """
    create the players table in the database.
    missing birthdate is the date of function execution so the "age" will be very low.
    null values of height, weight and foot are -1.
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :param dict_to_read: Dictionary to read instead of a json file, if it is not the initial creation of DB
    :return:
    """
    con = pymysql.connect(host, root, password, db=DB_NAME)
    with con.cursor() as cur:
        cur.execute(f"""USE {DB_NAME};""")

        cur.execute('''CREATE TABLE IF NOT EXISTS players (
                      id INT PRIMARY KEY AUTO_INCREMENT,
                      first_name VARCHAR(50) CHARACTER SET utf8mb4,
                      last_name VARCHAR(50) CHARACTER SET utf8mb4,
                      nationality VARCHAR(50) CHARACTER SET utf8mb4,
                      birthdate DATE,
                      birthplace VARCHAR(50) CHARACTER SET utf8mb4,              
                      position VARCHAR(50) CHARACTER SET utf8mb4, 
                      height FLOAT,
                      weight FLOAT,
                      foot_right BOOLEAN,
                      url VARCHAR(255))''')

        # Reading the default json file if no other dictionary was sent as an argument
        if dict_to_read:
            dict_df = pd.DataFrame(dict_to_read).T
        else:
            dict_df = pd.read_json("player.json").T

        dict_df = adjust_info(dict_df)
        logger.info("Start updating players table.")
        for ROW in dict_df.index:
            cur.execute("""INSERT INTO players (first_name, 
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
                        list(dict_df.loc[ROW, :]))
            if (ROW % 1000) == 0:
                con.commit()
                logger.info(f"Updated {ROW} players in the table.")
    con.commit()
    logger.warning(f"players table already exists.")
    r_num = pd.read_sql("SELECT COUNT(*) AS num FROM players;", con)
    logger.info(f"players table is up to date. \nThere are {int(r_num.num)} \
players in the table.")


def adjust_info(df):

    """
    Adjust the data frame to the players table format (DOB, height, weight, foot).
    :param df: Data frame to adjust
    :return df: Adjusted data frame
    """
    df = df.loc[:, df.columns != "Age"]
    df = df.loc[:, df.columns != "Place of birth"]

    # Convert birthday from string to date.
    for ind, date in zip(df.index, df["Date of birth"]):
        if str(date) == 'nan':
            df.loc[ind, "Date of birth"] = datetime.datetime.now()
        else:
            df.loc[ind, "Date of birth"] = datetime.datetime.strptime(str(date), "%d %B %Y")

    # remove height and weight extension (cm / kg)
    df["Height"][df["Height"].notna()] = \
        df["Height"][df["Height"].notna()].apply(lambda x: x[:-3])
    df["Height"][df["Height"].isna()] = 0

    df["Weight"][df["Weight"].notna()] = \
        df["Weight"][df["Weight"].notna()].apply(lambda x: x[:-3])
    df["Weight"][df["Weight"].isna()] = 0

    # convert right/left foot to boolean
    df['Foot'][df['Foot'].notnull()] = \
        df['Foot'][df['Foot'].notnull()] == 'Right'
    df['Foot'][df['Foot'].isnull()] = -1

    df = df[["First name", "Last name", "Nationality", "Date of birth",
             "Country of birth", "Position", "Height", "Weight", "Foot", "url"]]

    return df


def create_injuries(host, root, password, dict_to_read=None):
    """
    create the table containing injuries information in the database.
    null date are transformed to 01/01/1111
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :param dict_to_read: Dictionary to read instead of a json file, if it is not the initial creation of DB
    :return:
    """
    con = pymysql.connect(host, root, password, db=DB_NAME)
    with con.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS injuries (
                      id INT PRIMARY KEY AUTO_INCREMENT,
                      player_id INT,
                      description VARCHAR(50) CHARACTER SET utf8mb4,
                      start_date DATE,
                      end_date DATE,
                      FOREIGN KEY (player_id) 
                        REFERENCES players(id))''')

        # Reading the default json file if no other dictionary was sent as an argument
        if dict_to_read:
            dict_df = pd.DataFrame(dict_to_read).T
        else:
            dict_df = pd.read_json("injury.json").T

        dict_df = adjust_injuries(dict_df)
        logger.info("Start updating injuries table.")
        for ROW in dict_df.index:
            cur.execute("""INSERT INTO injuries 
                        (player_id, description, start_date, end_date)
                        VALUES (%s, %s, %s, %s)""", list(dict_df.loc[ROW, :]))

            if (ROW % 1000) == 0:
                con.commit()
                logger.info(f"updated {ROW} injuries in the table.")
    con.commit()
    logger.warning(f"injuries table already exists.")
    r_num = pd.read_sql("SELECT COUNT(*) AS num FROM injuries;", con)
    logger.info(f"injuries table is up to date. \nThere are {int(r_num.num)} injuries in the table")


def adjust_injuries(df):
    """
    Adjust the data frame to the injuries table format (player_id, start and end dates).
    :param df: Data frame to adjust
    :return df: Adjusted data frame
    """
    for ind, date in zip(df.index, df["start_date"]):
        df.loc[ind, "start_date"] = datetime.datetime.strptime(date, "%d/%m/%y")
        if df.loc[ind, "end_date"] is None:
            df.loc[ind, "end_date"] = datetime.datetime.strptime("01/01/1111", "%d/%m/%Y")
        else:
            df.loc[ind, "end_date"] = datetime.datetime.strptime(df.loc[ind, "end_date"], "%d/%m/%y")
    df["player_id"] += 1

    df = df[["player_id", "description", "start_date", "end_date"]]

    return df


def create_players_by_team(host, root, password, dict_to_read=None):
    """
    create the table that links player to team played by dates into the database.
    null dates are transformed to 01/01/70.
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :param dict_to_read: Dictionary to read instead of a json file, if it is not the initial creation of DB
    :return:
    """
    con = pymysql.connect(host, root, password, db=DB_NAME)
    with con.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS player_team (
                      id INT PRIMARY KEY AUTO_INCREMENT,
                      player_id INT,
                      team_id INT,
                      start_date DATE,
                      end_date DATE,
                        FOREIGN KEY (player_id) 
                        REFERENCES players(id),
                        FOREIGN KEY (team_id) 
                        REFERENCES teams(id))''')

        # Reading the default json file if no other dictionary was sent as an argument
        if dict_to_read:
            dict_df = pd.DataFrame(dict_to_read).T
        else:
            dict_df = pd.read_json("player_team.json").T

        dict_df = adjust_teams(dict_df)
        logger.info("start updating player_team table.")
        for ROW in dict_df.index:
            cur.execute("""INSERT INTO player_team 
                        (player_id,
                         team_id,
                         start_date,
                         end_date)
                         VALUES (%s, %s, %s, %s)""", list(dict_df.loc[ROW, :]))
            if (ROW % 1000) == 0:
                con.commit()
                logger.info(f"updated {ROW} fields in the player_team table.")
    con.commit()
    logger.warning(f"player_team table already exists.")
    r_num = pd.read_sql("SELECT COUNT(*) AS num FROM player_team;", con)
    logger.info(f"player_team table is up to date. \nThere are {int(r_num.num)} \
rows in the players_team table.")


def adjust_teams(df):
    """
    Adjust the data frame to the player teams table format
    (player_id, team_id, start and end dates).
    :param df: Data frame to adjust
    :return df: Adjusted data frame
    """
    df = df[["player_id", "team_id", "start", "end"]]
    df = df[df["team_id"].notna()]

    df["player_id"] += 1
    df["team_id"] += 1
    df["end"].fillna("01/01/70", inplace=True)

    for i in df.index:
        df.loc[i, "start"] = datetime.datetime.strptime(df.loc[i, "start"], "%d/%m/%y")
        df.loc[i, "end"] = datetime.datetime.strptime(df.loc[i, "end"], "%d/%m/%y")

    df.index = range(df.shape[0])

    df = df[["player_id", "team_id", "start", "end"]]

    return df


def create_players_by_season(host, root, password, dict_to_read=None):
    """
    create a table lists player activity by season into the database.
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :param dict_to_read: Dictionary to read instead of a json file, if it is not the initial creation of DB
    :return:
    """
    con = pymysql.connect(host, root, password, db=DB_NAME)
    with con.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS player_season (
                      id INT PRIMARY KEY AUTO_INCREMENT,
                      player_id INT,
                      team_id INT,
                      season VARCHAR(9),
                      Minutes_played INT,
                      Appearances INT,
                      Lineups INT,
                      Substitute_in INT,
                      Substitute_out INT,
                      on_bench INT,
                      Goal INT,
                      Yellow_card INT,
                      Yellow_2nd INT,
                      Red_card INT,
                        FOREIGN KEY (player_id) 
                        REFERENCES players(id),
                        FOREIGN KEY (team_id) 
                        REFERENCES teams(id))''')

        # Reading the default json file if no other dictionary was sent as an argument
        if dict_to_read:
            dict_df = pd.DataFrame(dict_to_read).T
        else:
            dict_df = pd.read_json("player_season.json").T

        dict_df = adjust_seasons(dict_df)
        logger.info("start updating player_season table.")
        for ROW in dict_df.index:
            cur.execute("""INSERT INTO player_season 
                        (player_id,
                          team_id,
                          season,
                          Minutes_played,
                          Appearances,
                          Lineups,
                          Substitute_in,
                          Substitute_out,
                          on_bench,
                          Goal,
                          Yellow_card,
                          Yellow_2nd,
                          Red_card)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        list(dict_df.loc[ROW, :]))
            if (ROW % 1000) == 0:
                con.commit()
                logger.info(f"updated {ROW} fields in the player_season table.")
    con.commit()
    logger.warning(f"player_season table already exists.")
    r_num = pd.read_sql("SELECT COUNT(*) AS num FROM player_season;", con)
    logger.info(f"player_season table is up to date. \nThere are {int(r_num.num)} \
rows in the players_season table.")


def adjust_seasons(df):
    """
    Adjust the data frame to the player seasons table format
    (player_id, team_id, all season's statistics and dealing with NaN).
    :param df: Data frame to adjust
    :return df: Adjusted data frame
    """
    df = df[df["Team_id"].notna()]
    df = df[["player_id", "Team_id", "Season", "Minutes played", "Appearances",
             "Lineups", "Substitute in", "Substitute out", "Substitutes on bench",
             "Goal", "Yellow card", "Yellow 2nd/RC", "Red card"]]
    df["player_id"] += 1
    df["Team_id"] += 1
    df[df == '?'] = -1

    return df


logger = define_logger(LOGGER, LOGGER_FILE)


@click.command()
@click.option('--host', '-h', default='localhost', prompt='Please insert the MySQL host name:', help='MySQL host name')
@click.option('--root', '-r', default='root', prompt='Please insert the MySQL root:', help='MySQL root')
@click.option('--password', '-ps', default='', prompt='Please insert the MySQL password:', help='MySQL password')
def main(host, root, password):
    """
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return:
    """
    create_db(host, root, password)
    create_leagues(host, root, password)
    create_teams(host, root, password)
    create_players(host, root, password)
    create_injuries(host, root, password)
    create_players_by_team(host, root, password)
    create_players_by_season(host, root, password)


if __name__ == "__main__":
    main()
