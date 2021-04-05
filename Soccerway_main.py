
from find_and_add_functions import find_league, find_team, find_player
from Soccerway_create_db import define_logger
import config
import click

logger = define_logger(config.LOGGER, config.LOG_FILE)

def find_from_input(input_str, input_type, host, root, password, fun="find_league"):
    """
    assisting function that loops over values (separated by ",") and run a function using them.
    :param input_str: a string containing values separated by ",".
    :param input_type: a string that clarifies if the input is a url/ name/ country (for leagues only)
    :param host: MySQL host name.
    :param root: MySQL root name.
    :param password: MySQL password.
    :param fun: the function to run (accept value and table).
    :return: Results of queries.
    """
    results = 'Nothing happened'

    if input_str:
        input_set = set(map(lambda x: x.lstrip(), input_str.title().split(',')))
        results = eval(f"{fun}(input_set, input_type, host, root, password)")

    return results


@click.command()
@click.option('--url', '-u', help='url of additional league to scrape, can be multiple urls separated by comma only.')
@click.option('--country', '-c', help='Country of additional league to scrape, \
                                       can be multiple countries separated by comma only.')
@click.option('--league', '-l', help='Name of league to scrape, can be multiple names separated by comma only.')
@click.option('--team', '-t', help='Name of team to scrape, can be multiple teams separated by comma only.')
@click.option('--player', '-p', help='Name of player to scrape, can be multiple names separated by comma only.')
@click.option('--host', '-h', default='localhost', prompt='Please insert the MySQL host name:', help='MySQL host name')
@click.option('--root', '-r', default='root', prompt='Please insert the MySQL root:', help='MySQL root')
@click.option('--password', '-ps', default='', prompt='Please insert the MySQL password:', help='MySQL password')
def main(url, country, league, team, player, host, root, password):
    """
    main function that runs the scraping
    # :param number: number of top leagues user want to extract.
    :param url: a string of urls of leagues (separated by ",").
    :param country:  a string of country names (separated by ",").
    :param league: a string of leagues names (separated by ",").
    :param team: a string of teams names (separated by ",").
    :param player: a string of players names (separated by ",").
    :param host: MySQL host name
    :param root: MySQL root name
    :param password: MySQL password
    :return:
    """
    logger.info(find_from_input(url,     'url',     host, root, password, fun="find_league"))
    logger.info(find_from_input(country, 'country', host, root, password, fun="find_league"))
    logger.info(find_from_input(league,  'name',    host, root, password, fun="find_league"))
    logger.info(find_from_input(team,    'name',    host, root, password, fun="find_team"))
    logger.info(find_from_input(player,  'name',    host, root, password, fun="find_player"))


if __name__ == '__main__':
    main()
