import pandas as pd
import numpy as np
import os

#load data
file_path_2019_2020 = '../0_datasets/processed/stats/stats_2019_2020.csv'
file_path_2020_2021 = '../0_datasets/processed/stats/stats_2020_2021.csv'
file_path_2021_2022 = '../0_datasets/processed/stats/stats_2021_2022.csv'
file_path_2022_2023 = '../0_datasets/processed/stats/stats_2022_2023.csv'

df_2019_2020 = pd.read_csv(file_path_2019_2020)
df_2020_2021 = pd.read_csv(file_path_2020_2021)
df_2021_2022 = pd.read_csv(file_path_2021_2022)
df_2022_2023 = pd.read_csv(file_path_2022_2023)

#To track the number of rows dropped
print(len(df_2019_2020))
print('----------------')
print(len(df_2020_2021))
print('----------------')
print(len(df_2021_2022))
print('----------------')
print(len(df_2022_2023))
print('****************')

#Check for empty player_uuid
df_2019_2020 = df_2019_2020[df_2019_2020['player_uuid'] != '']
df_2020_2021 = df_2020_2021[df_2020_2021['player_uuid'] != '']
df_2021_2022 = df_2021_2022[df_2021_2022['player_uuid'] != '']
df_2022_2023 = df_2022_2023[df_2022_2023['player_uuid'] != '']

df_2019_2020 = df_2019_2020[df_2019_2020['player_uuid'].notnull()]
df_2020_2021 = df_2020_2021[df_2020_2021['player_uuid'].notnull()]
df_2021_2022 = df_2021_2022[df_2021_2022['player_uuid'].notnull()]
df_2022_2023 = df_2022_2023[df_2022_2023['player_uuid'].notnull()]

#Check for empty team_uuid
df_2019_2020 = df_2019_2020[df_2019_2020['team_uuid'] != '']
df_2020_2021 = df_2020_2021[df_2020_2021['team_uuid'] != '']
df_2021_2022 = df_2021_2022[df_2021_2022['team_uuid'] != '']
df_2022_2023 = df_2022_2023[df_2022_2023['team_uuid'] != '']

df_2019_2020 = df_2019_2020[df_2019_2020['team_uuid'].notnull()]
df_2020_2021 = df_2020_2021[df_2020_2021['team_uuid'].notnull()]
df_2021_2022 = df_2021_2022[df_2021_2022['team_uuid'].notnull()]
df_2022_2023 = df_2022_2023[df_2022_2023['team_uuid'].notnull()]

#To track the number of rows dropped
print(len(df_2019_2020))
print('----------------')
print(len(df_2020_2021))
print('----------------')
print(len(df_2021_2022))
print('----------------')
print(len(df_2022_2023))
print('****************')

#Create folder if it doesn't exist
if not os.path.exists('../0_datasets/cleaned/stats'):
    os.makedirs('../0_datasets/cleaned/stats')

#Save data
df_2019_2020.to_csv('../0_datasets/cleaned/stats/stats_2019_2020.csv', index=False)
df_2020_2021.to_csv('../0_datasets/cleaned/stats/stats_2020_2021.csv', index=False)
df_2021_2022.to_csv('../0_datasets/cleaned/stats/stats_2021_2022.csv', index=False)
df_2022_2023.to_csv('../0_datasets/cleaned/stats/stats_2022_2023.csv', index=False)

