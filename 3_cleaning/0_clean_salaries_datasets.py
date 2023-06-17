import pandas as pd
import numpy as np

file_path_2019_2020 = '../0_datasets/processed/salaries/salaries_2019_2020.csv'
file_path_2020_2021 = '../0_datasets/processed/salaries/salaries_2020_2021.csv'
file_path_2021_2022 = '../0_datasets/processed/salaries/salaries_2021_2022.csv'
file_path_2022_2023 = '../0_datasets/processed/salaries/salaries_2022_2023.csv'

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

df_2019_2020 = df_2019_2020[df_2019_2020['player_uuid'] != '']
df_2020_2021 = df_2020_2021[df_2020_2021['player_uuid'] != '']
df_2021_2022 = df_2021_2022[df_2021_2022['player_uuid'] != '']
df_2022_2023 = df_2022_2023[df_2022_2023['player_uuid'] != '']

df_2019_2020 = df_2019_2020[df_2019_2020['player_uuid'].notnull()]
df_2020_2021 = df_2020_2021[df_2020_2021['player_uuid'].notnull()]
df_2021_2022 = df_2021_2022[df_2021_2022['player_uuid'].notnull()]
df_2022_2023 = df_2022_2023[df_2022_2023['player_uuid'].notnull()]

#To track the number of rows dropped
print(len(df_2019_2020))
print('----------------')
print(len(df_2020_2021))
print('----------------')
print(len(df_2021_2022))
print('----------------')
print(len(df_2022_2023))
print('----------------')

#create folder if it doesn't exist
import os
if not os.path.exists('../0_datasets/cleaned/salaries'):
    os.makedirs('../0_datasets/cleaned/salaries')

# save to csv
df_2019_2020.to_csv('../0_datasets/cleaned/salaries/salaries_2019_2020.csv', index=False)
df_2020_2021.to_csv('../0_datasets/cleaned/salaries/salaries_2020_2021.csv', index=False)
df_2021_2022.to_csv('../0_datasets/cleaned/salaries/salaries_2021_2022.csv', index=False)
df_2022_2023.to_csv('../0_datasets/cleaned/salaries/salaries_2022_2023.csv', index=False)

