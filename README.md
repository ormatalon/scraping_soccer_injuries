
We are interested in investigating soccer injuries and absence.

The first step of the process is finding data regarding injuries and absence.
We find this in the website https://int.soccerway.com/.

### requirements.txt:
The file contains all packages and used in our scripts.


### scraping_functions.py:
This file scrapes data regarding soccer players' injuries from the aforementioned website.
It allows saving 6 JSON files:
1. league.json - Contain links for all top N soccer leagues (most popular ones).
2. team.json - Contain links for all teams within each of these leagues.
3. player.json - Contain links, personal information and injuries data of all players.
4. injury.json - Contains all players' injuries info (types and dates).
5. player_team - Contains all teams players have played for.
6. player_season - Contains info and statistics of all seasons player have participated in.

These 6 JSON files can be found in the folder.


### Soccerway_create_db.py:
This file converts the json data contained in the JSON files into MySQL database.
The database's tables have the same names as the JSON files (totally 6).


### Soccerway_main.py:
This file enables performing queries to the DB created.
Currently, the following options are available:
1. Finding a league according to its name/ country/ URL.
2. Finding a team by name.
3. Finding a player by name.

If no results are found, the module uses the "additive" functions of 
'find_and_add_functions' to scrape the required data from 'soccerway.com', 
add it to the DB, and return the new results.


### find_and_add_functions.py:
Functions used to query the DB while looking for results,
and if none are found - scrape 'soccerway.com' to add them.
It's important to notice if a team or player wasn't found on the DB - the entire
league for which it belongs is being added (all teams and players from that league),
to avoid cases of NaN id values (league_id/ team_id).


### config.py:
General constants used throughout the project.


### soccer_api.py:
Enables adding players (only!) of a chosen league into the DB, using "SportsRadar" API.
If players are already found in the DB - they aren't added.
Its methods are not available through the main project's CLI (only as separated 
additional options).


### soccer_injuries_database_diagram.png:
A diagram illustrating the database with all its charts.


### Scraping pipeline:

1. run scraping_functions_py - this will take a few hours and create the json files with the data.
* It can not be ran since soccerway.com do not allow scraping anymore. Therefore the json files already exist.

2. run Soccerway_create_db.py - this file create the database "soccer_injuries" given the json files exist.
After running the script the user is asked to insert mysql host name, root and password.

3. run Soccerway_main.py - In case one wants to add a league, team or player, the file can be ran.
running the command python Soccerway_main.py --help gives the options to add the league, team or player.
* It can not be ran since soccerway.com do not allow scraping anymore.
