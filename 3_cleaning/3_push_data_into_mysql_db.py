import pandas as pd
import numpy as np
from mysql.connector import Error
import mysql.connector
import SQL_SERVER
import os

def create_database_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

#Connect to MySQL server
connection = create_database_connection(SQL_SERVER.SERVER, SQL_SERVER.USERNAME, SQL_SERVER.PASSWORD, SQL_SERVER.DATABASE)

# load players data
file_path_players = '../0_datasets/processed/unique_players.csv'
df_players = pd.read_csv(file_path_players)

for index, row in df_players.iterrows():
    query = f"""
        INSERT INTO players (id, name) VALUES ('{row['uuid']}', '{row['NAME']}');
    """

    execute_query(connection, query)

# load teams data
file_path_teams = '../0_datasets/processed/teams_with_uuid.csv'
df_teams = pd.read_csv(file_path_teams)
for index, row in df_teams.iterrows():
    query = f"""
        INSERT INTO teams (id, name, prefix_1, prefix_2) VALUES ('{row['uuid']}', '{row['name']}', '{row['prefix_1']}', '{row['prefix_2']}');
    """

    execute_query(connection, query)

# load champions data
file_path_champions = '../0_datasets/processed/champions_with_team_ids.csv'
df_champs = pd.read_csv(file_path_champions)
for index, row in df_champs.iterrows():
    query = f"""
        INSERT INTO champions (id, year, region, champion_uuid) 
        VALUES ('{row['uuid']}', {row['year']}, '{row['region']}', '{row['champion_uuid']}');
    """

    execute_query(connection, query)

# Load salaries data
file_path_salaries_2019_2020 = '../0_datasets/cleaned/salaries/salaries_2019_2020.csv'
file_path_salaries_2020_2021 = '../0_datasets/cleaned/salaries/salaries_2020_2021.csv'
file_path_salaries_2021_2022 = '../0_datasets/cleaned/salaries/salaries_2021_2022.csv'
file_path_salaries_2022_2023 = '../0_datasets/cleaned/salaries/salaries_2022_2023.csv'

df_salaries_2019_2020 = pd.read_csv(file_path_salaries_2019_2020)
df_salaries_2020_2021 = pd.read_csv(file_path_salaries_2020_2021)
df_salaries_2021_2022 = pd.read_csv(file_path_salaries_2021_2022)
df_salaries_2022_2023 = pd.read_csv(file_path_salaries_2022_2023)

#Insert salaries data
for index, row in df_salaries_2019_2020.iterrows():
    query = f"""
        INSERT INTO salaries (id, player_uuid, season, salary_in_usd) 
        VALUES ('{row['uuid']}', '{row['player_uuid']}', '2019-2020', {row['salary_in_usd']});
    """

    execute_query(connection, query)

for index, row in df_salaries_2020_2021.iterrows():
    query = f"""
        INSERT INTO salaries (id, player_uuid, season, salary_in_usd) 
        VALUES ('{row['uuid']}', '{row['player_uuid']}', '2020-2021', {row['salary_in_usd']});
    """

    execute_query(connection, query)

for index, row in df_salaries_2021_2022.iterrows():
    query = f"""
        INSERT INTO salaries (id, player_uuid, season, salary_in_usd) 
        VALUES ('{row['uuid']}', '{row['player_uuid']}', '2021-2022', {row['salary_in_usd']});
    """

    execute_query(connection, query)

for index, row in df_salaries_2022_2023.iterrows():
    query = f"""
        INSERT INTO salaries (id, player_uuid, season, salary_in_usd) 
        VALUES ('{row['uuid']}', '{row['player_uuid']}', '2022-2023', {row['salary_in_usd']});
    """

    execute_query(connection, query)

#Load stats data
file_path_stats_2019_2020 = '../0_datasets/cleaned/stats/stats_2019_2020.csv'
file_path_stats_2020_2021 = '../0_datasets/cleaned/stats/stats_2020_2021.csv'
file_path_stats_2021_2022 = '../0_datasets/cleaned/stats/stats_2021_2022.csv'
file_path_stats_2022_2023 = '../0_datasets/cleaned/stats/stats_2022_2023.csv'

df_stats_2019_2020 = pd.read_csv(file_path_stats_2019_2020)
df_stats_2020_2021 = pd.read_csv(file_path_stats_2020_2021)
df_stats_2021_2022 = pd.read_csv(file_path_stats_2021_2022)
df_stats_2022_2023 = pd.read_csv(file_path_stats_2022_2023)

# Insert stats data
for index, row in df_stats_2019_2020.iterrows():
    query = f"""
        INSERT INTO stats (
            id, team_uuid, player_uuid, RANK, POS, AGE, GP, MPG, USG_PCT, TO_PCT, FTA, FT_PCT,
            2PA, 2P_PCT, 3PA, 3P_PCT, eFG_PCT, TS_PCT, PPG, RPG, APG, P_A, SPG, BPG, TPG, VI, 
            ORtg, DRtg, season
        ) VALUES (
            '{row['uuid']}', '{row['team_uuid']}', '{row['player_uuid']}', {row['RANK']}, 
            '{row['POS']}', {row['AGE']}, {row['GP']}, {row['MPG']}, {row['USG%']}, 
            {row['TO%']}, {row['FTA']}, {row['FT%']}, {row['2PA']}, {row['2P%']}, 
            {row['3PA']}, {row['3P%']}, {row['eFG%']}, {row['TS%']}, {row['PPG']}, 
            {row['RPG']}, {row['APG']}, {row['P+A']}, {row['SPG']}, {row['BPG']}, 
            {row['TPG']}, {row['VI']}, {row['ORtg']}, {row['DRtg']}, '2019-2020'
        );
    """

    execute_query(connection, query)

for index, row in df_stats_2020_2021.iterrows():
    query = f"""
        INSERT INTO stats (
            id, team_uuid, player_uuid, RANK, POS, AGE, GP, MPG, USG_PCT, TO_PCT, FTA, FT_PCT,
            2PA, 2P_PCT, 3PA, 3P_PCT, eFG_PCT, TS_PCT, PPG, RPG, APG, P_A, SPG, BPG, TPG, VI, 
            ORtg, DRtg, season
        ) VALUES (
            '{row['uuid']}', '{row['team_uuid']}', '{row['player_uuid']}', {row['RANK']}, 
            '{row['POS']}', {row['AGE']}, {row['GP']}, {row['MPG']}, {row['USG%']}, 
            {row['TO%']}, {row['FTA']}, {row['FT%']}, {row['2PA']}, {row['2P%']}, 
            {row['3PA']}, {row['3P%']}, {row['eFG%']}, {row['TS%']}, {row['PPG']}, 
            {row['RPG']}, {row['APG']}, {row['P+A']}, {row['SPG']}, {row['BPG']}, 
            {row['TPG']}, {row['VI']}, {row['ORtg']}, {row['DRtg']}, '2020-2021'
        );
    """

    execute_query(connection, query)

for index, row in df_stats_2021_2022.iterrows():
    query = f"""
        INSERT INTO stats (
            id, team_uuid, player_uuid, RANK, POS, AGE, GP, MPG, USG_PCT, TO_PCT, FTA, FT_PCT,
            2PA, 2P_PCT, 3PA, 3P_PCT, eFG_PCT, TS_PCT, PPG, RPG, APG, P_A, SPG, BPG, TPG, VI, 
            ORtg, DRtg, season
        ) VALUES (
            '{row['uuid']}', '{row['team_uuid']}', '{row['player_uuid']}', {row['RANK']}, 
            '{row['POS']}', {row['AGE']}, {row['GP']}, {row['MPG']}, {row['USG%']}, 
            {row['TO%']}, {row['FTA']}, {row['FT%']}, {row['2PA']}, {row['2P%']}, 
            {row['3PA']}, {row['3P%']}, {row['eFG%']}, {row['TS%']}, {row['PPG']}, 
            {row['RPG']}, {row['APG']}, {row['P+A']}, {row['SPG']}, {row['BPG']}, 
            {row['TPG']}, {row['VI']}, {row['ORtg']}, {row['DRtg']}, '2021-2022'
        );
    """

    execute_query(connection, query)

for index, row in df_stats_2022_2023.iterrows():
    query = f"""
        INSERT INTO stats (
            id, team_uuid, player_uuid, RANK, POS, AGE, GP, MPG, USG_PCT, TO_PCT, FTA, FT_PCT,
            2PA, 2P_PCT, 3PA, 3P_PCT, eFG_PCT, TS_PCT, PPG, RPG, APG, P_A, SPG, BPG, TPG, VI, 
            ORtg, DRtg, season
        ) VALUES (
            '{row['uuid']}', '{row['team_uuid']}', '{row['player_uuid']}', {row['RANK']}, 
            '{row['POS']}', {row['AGE']}, {row['GP']}, {row['MPG']}, {row['USG%']}, 
            {row['TO%']}, {row['FTA']}, {row['FT%']}, {row['2PA']}, {row['2P%']}, 
            {row['3PA']}, {row['3P%']}, {row['eFG%']}, {row['TS%']}, {row['PPG']}, 
            {row['RPG']}, {row['APG']}, {row['P+A']}, {row['SPG']}, {row['BPG']}, 
            {row['TPG']}, {row['VI']}, {row['ORtg']}, {row['DRtg']}, '2022-2023'
        );
    """

    execute_query(connection, query)
