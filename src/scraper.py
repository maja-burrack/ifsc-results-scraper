import requests
import pandas as pd
import yaml
import brotli
import gzip
import json
import time

from typing import List

import os
from dotenv import load_dotenv

class IFSCResultsScraper:
    def __init__(self, baseurl: str, headers: dict):
        self.baseurl = baseurl
        self.headers = headers

    def _test_api(self, test_season_id=37):
        url = self.baseurl + f"seasons/{test_season_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            print("Response OK!")
        else:
            print(f"ERROR! Status code {response.status_code}")
    
    def get_event_ids(self, year):
        season_id = self._map_year_to_season_id(year)
        url = self.baseurl + f"seasons/{season_id}"
        
        response = requests.get(url, headers=self.headers)
        assert response.status_code == 200, f"Error code {response.status_code}!"
        
        decoded = self._decompress_response(response)
        
        r = json.loads(decoded)

        league_id = r['leagues'][0]['url'].split('/')[-1]

        event_ids = [
            event['event_id']
            for event in r['events']
            if event['league_season_id'] == int(league_id)
        ]
        
        return event_ids

    @staticmethod
    def _decompress_response(response):
        raw = response.content
        encoding = response.headers.get("Content-Encoding")
        
        try:
            if encoding == "br":
                decoded = brotli.decompress(raw)
            elif encoding == "gzip":
                decoded = gzip.decompress(raw)
            else:
                decoded = raw
        except Exception as e:
            decoded = raw
        return decoded

    @staticmethod
    def _map_year_to_season_id(year: int):
        start_year = 1990
        season_id = year - start_year + 2
        return season_id 

    def get_event_dcat_ids(self, event_id: int):
        url = self.baseurl + f"events/{event_id}"
        response = requests.get(url, headers=self.headers)
        
        response = self._decompress_response(response)
        response = json.loads(response)
        
        # get ids for dcats
        dcat_ids = [
            dcat['dcat_id'] for dcat in response['dcats']
        ]

        return dcat_ids

    def get_event_results(self, event_id: int):
        dcat_ids = self.get_event_dcat_ids(event_id)
        
        results_lst = []
        for dcat_id in dcat_ids:
            url = self.baseurl + f"/events/{event_id}/result/{dcat_id}"
            results = json.loads(self._decompress_response(requests.get(url, headers=self.headers)))
            results['dcat_id'] = dcat_id
            results_lst.append(results)
        
        responses = {'event_id': event_id, 'results': results_lst}
        
        return responses

    def get_athlete_info(self, athlete_id: int):
        url = self.baseurl + f"athletes/{athlete_id}"
        response= requests.get(url, headers=self.headers)
        athlete_info = json.loads(self._decompress_response(response))
        return athlete_info

    def get_athlete_info_multiple(self, athlete_ids: List[int]):
        athlete_info = {}
        for athlete_id in athlete_ids:
            athlete_info[athlete_id] = self.get_athlete_info(athlete_id)
        return athlete_info

    def fetch_data(self, year=2025):
        event_ids = self.get_event_ids(year=year)
        
        all_event_results = []
        for event_id in event_ids:
            event_results = self.get_event_results(event_id=event_id)
            all_event_results.append(event_results)
        
        return all_event_results

    @staticmethod
    def parse_data(data):
        df = pd.DataFrame(data)
        
        df = df.explode('results')
        df['event_name'] = df['results'].apply(lambda x: x['event'])
        df['dcat_id'] = df['results'].apply(lambda x: x['dcat_id'])
        df['dcat'] = df['results'].apply(lambda x: x['dcat'])
        df['status'] = df['results'].apply(lambda x: x['status'])
        df['status_as_of'] = df['results'].apply(lambda x: x['status_as_of'])
        
        df = df[df['status']=='finished']
        
        df['ranking'] = df['results'].apply(lambda x: x['ranking'])
        df = df.explode('ranking')
        
        df['athlete_id'] = df['ranking'].apply(lambda x: x['athlete_id'])
        df['athlete_name'] = df['ranking'].apply(lambda x: x['name'])
        df['athlete_country'] = df['ranking'].apply(lambda x: x['country'])
        df['comp_rank'] = df['ranking'].apply(lambda x: x['rank'])
        df['rounds'] = df['ranking'].apply(lambda x: x['rounds'])
        
        df = df.explode('rounds')
        df['round'] = df['rounds'].apply(lambda x: x['round_name'])
        df['score'] = df['rounds'].apply(lambda x: x['score'])
        df['round_rank'] = df['rounds'].apply(lambda x: x['rank'])

        df = df.drop(['ranking', 'rounds', 'results'], axis=1)
        
        return df

    def transform_data(self, df, only_finalists=True):
        df = df.loc[df['dcat'].str.contains('boulder', case=False)]
        
        df['comp_id'] = df['event_id'].astype(str) + df['dcat_id'].astype(str)
        
        if only_finalists:
            athletes_in_final = df[df['round']=='Final'][['comp_id', 'athlete_id']].drop_duplicates()
            df = df.merge(athletes_in_final, on=['comp_id', 'athlete_id'], how='inner')
        
        df = df.drop('status', axis=1)
        
        df = self._enrich_with_athlete_data(df)
        
        return df

    def _enrich_with_athlete_data(self, df):
        athlete_ids = df['athlete_id'].unique().tolist()
    
        athlete_info = self.get_athlete_info_multiple(athlete_ids=athlete_ids)
        
        athlete_df = pd.DataFrame(athlete_info.values())
        
        athlete_df.rename({'id': 'athlete_id'}, axis=1, inplace=True)
        
        athlete_df['first_season'] = athlete_df['all_results'].apply(lambda x: min([int(i['season']) for i in x]))
        
        athlete_df = athlete_df.loc[:, ["athlete_id", "birthday", "gender", "first_season", "height", "arm_span"]]
        
        df_enriched = df.merge(athlete_df, on="athlete_id", how='left')
        
        return df_enriched

    
if __name__ == "__main__":
    
    with open("../config.yaml", "r") as f:
        config = yaml.safe_load(f)

    load_dotenv()

    BASEURL = config['baseurl']
    YEAR = config['year']
    COOKIE = os.getenv('COOKIE')
    
    headers = config['headers']
    headers['cookie'] = COOKIE
    
    scraper = IFSCResultsScraper(headers=headers, baseurl=BASEURL)
    
    data = scraper.fetch_data(year=YEAR)
    df = scraper.parse_data(data)
    df_transformed = scraper.transform_data(df, only_finalists=True)

    folder_dest = "../data"
    if not os.path.isdir(folder_dest):
        os.makedirs(folder_dest)
        
    output_file_name = f"{folder_dest}/ifsc_boulder_results_{YEAR}.csv"
    df_transformed.to_csv(output_file_name, index=False)
      
