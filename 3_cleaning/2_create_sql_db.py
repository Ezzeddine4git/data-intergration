import mysql.connector
from mysql.connector import Error
import SQL_SERVER

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

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
connection = create_server_connection(SQL_SERVER.SERVER, SQL_SERVER.USERNAME, SQL_SERVER.PASSWORD)

#Create database
create_database_query = "CREATE DATABASE data_integration"
create_database(connection, create_database_query)

#Connect to database
db_connection = create_database_connection(SQL_SERVER.SERVER, SQL_SERVER.USERNAME, SQL_SERVER.PASSWORD, SQL_SERVER.DATABASE)

#Create teams table
create_teams_table_query = """
    CREATE TABLE teams (
        id CHAR(36) PRIMARY KEY,
        name TEXT,
        prefix_1 TEXT,
        prefix_2 TEXT
    );
"""
execute_query(db_connection, create_teams_table_query)

#Create champions table
create_champions_table_query = """
    CREATE TABLE champions (
        id CHAR(36) PRIMARY KEY,
        year INT(4) NOT NULL,
        region ENUM('western', 'eastern') NOT NULL,
        champion_uuid CHAR(36) NOT NULL,
        FOREIGN KEY (champion_uuid) REFERENCES teams(id)
    );
"""
execute_query(db_connection, create_champions_table_query)

#Create players table
create_players_table_query = """
    CREATE TABLE players (
        id CHAR(36) PRIMARY KEY,
        name TEXT NOT NULL
    );
"""
execute_query(db_connection, create_players_table_query)

# Create salaries table
create_salaries_table_query = """ 
    CREATE TABLE salaries (
        id CHAR(36) PRIMARY KEY,
        player_uuid CHAR(36) NOT NULL,
        FOREIGN KEY (player_uuid) REFERENCES players(id),
        salary_in_usd FLOAT NOT NULL,
        season TEXT NOT NULL      
    );
"""
execute_query(db_connection, create_salaries_table_query)

#Create stats table
create_stats_table_query = """
    CREATE TABLE stats (
    id CHAR(36) PRIMARY KEY,
    team_uuid CHAR(36) NOT NULL,
    FOREIGN KEY (team_uuid) REFERENCES teams(id),
    player_uuid CHAR(36),
    FOREIGN KEY (player_uuid) REFERENCES players(id),
    RANK INT,
    POS CHAR(2),
    AGE FLOAT,
    GP INT,
    MPG FLOAT,
    USG_PCT FLOAT,
    TO_PCT FLOAT,
    FTA INT,
    FT_PCT FLOAT,
    2PA INT,
    2P_PCT FLOAT,
    3PA INT,
    3P_PCT FLOAT,
    eFG_PCT FLOAT,
    TS_PCT FLOAT,
    PPG FLOAT,
    RPG FLOAT,
    APG FLOAT,
    P_A FLOAT,
    SPG FLOAT,
    BPG FLOAT,
    TPG FLOAT,
    VI FLOAT,
    ORtg FLOAT,
    DRtg FLOAT,
    season TEXT
);
"""
execute_query(db_connection, create_stats_table_query)