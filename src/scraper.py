import requests
import pandas as pd
import yaml

from typing import List

import os
from dotenv import load_dotenv

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

load_dotenv()

BASEURL = config['baseurl']
COOKIE = os.getenv('COOKIE')

headers = {
    "accept": "application/json",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-GB,en;q=0.6",
    "cookie": COOKIE,
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36"
}

def get_event_ids(year=2025, headers=headers):
    season_id = _map_year_to_season_id(year)
    url = BASEURL + f"seasons/{season_id}"
    response = requests.get(url, headers=headers)
    
    r = response.json()
    print(r)
    league_id = r['leagues'][0]['url'].split('/')[-1]
    

    event_ids = [
        event['event_id']
        for event in r['events']
        if event['league_season_id'] == int(league_id)
    ]
    
    return event_ids

def _map_year_to_season_id(year):
    start_year = 1990
    season_id = year - start_year + 2
    return season_id 

def get_event_dcat_ids(event_id, headers=headers):
    url = BASEURL + f"events/{event_id}"
    response = requests.get(url, headers=headers).json()
    
    # get ids for dcats
    dcat_ids = [
        dcat['dcat_id'] for dcat in response['dcats']
    ]

    return dcat_ids

def get_event_results(event_id, headers=headers):
    dcat_ids = get_event_dcat_ids(event_id)
    
    responses = {'event_id': event_id}
    for dcat_id in dcat_ids:
        url = BASEURL + f"{event_id}/result/{dcat_id}"
        results = requests.get(url, headers=headers).json()
        results['dcat_id'] = dcat_id
        responses['results'] = results
    
    return responses

def get_athlete_info(athlete_id: int, headers=headers):
    url = BASEURL + f"athletes/{athlete_id}"
    athlete_info = requests.get(url, headers=headers).json()
    return athlete_info

def get_athlete_info_multiple(athlete_ids: List[int], headers=headers):
    athlete_info = {}
    for athlete_id in athlete_ids:
        athlete_info[athlete_id] = get_athlete_info(athlete_id, headers=headers)

def fetch_data(year=2025, headers=headers):
    event_ids = get_event_ids(year=year, headers=headers)
    
    all_event_results = []
    for event_id in event_ids:
        event_results = get_event_results(event_id=event_id, headers=headers)
        all_event_results.append(event_results)
    
    return all_event_results

def parse_data(data):
    df = pd.DataFrame(data)
    
    df['event_name'] = df['results'].apply(lambda x: x['event'])
    df['dcat_id'] = df['results'].apply(lambda x: x['dcat_id'])
    df['dcat'] = df['results'].apply(lambda x: x['dcat'])
    df['status'] = df['results'].apply(lambda x: x['status'])
    
    df = df[df['status']=='finished']
    
    df['ranking'] = df['results'].apply(lambda x: x['ranking'])
    df = df.explode('ranking')
    
    df['athlete_id'] = df['ranking'].apply(lambda x: x['athlete_id'])
    df['athlete_name'] = df['ranking'].apply(lambda x: x['name'])
    df['athlete_country'] = df['ranking'].apply(lambda x: x['country'])
    df['rounds'] = df['ranking'].apply(lambda x: x['rounds'])
    
    df = df.explode('rounds')
    df['round'] = df['rounds'].apply(lambda x: x['round_name'])
    df['score'] = df['rounds'].apply(lambda x: x['score'])

    df = df.drop(['ranking', 'rounds', 'results'], axis=1)
    
    athlete_ids = df['athlete_id'].unique().to_list()
    athlete_info = get_athlete_info_multiple(athlete_ids=athlete_ids)
    
    return df

def transform_data(df):
    df = df.loc[df['dcat'].str.contains('boulder', case=False)]
    
    df['comp_id'] = df['event_id'].astype(str) + df['dcat_id'].astype(str)
    
    athletes_in_final = df[df['round']=='Final'][['comp_id', 'athlete_id']].drop_duplicates()
    df = df.merge(athletes_in_final, on=['comp_id', 'athlete_id'], how='inner')
    
    df = df.drop('status', axis=1)
    
    return df
