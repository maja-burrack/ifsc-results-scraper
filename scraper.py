import requests
import pandas as pd

url = "https://ifsc.results.info/api/v1/seasons/37"
baseurl = "https://ifsc.results.info"

headers = {
    "accept": "application/json",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-GB,en;q=0.6",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36"
}

# TODO: add cookie to header

def get_event_ids():
    url = baseurl + "/api/v1/seasons/37"
    response = requests.get(url, headers=headers)
    
    r = response.json()
    league_id = r['leagues'][0]['url'].split('/')[-1]
    

    event_ids = [
        event['event_id']
        for event in r['events']
        if event['league_season_id'] == int(league_id)
    ]
    
    return event_ids

def get_event_dcat_ids(event_id):
    url = baseurl + f"/api/v1/events/{event_id}"
    response = requests.get(url, headers=headers).json()
    
    # get ids for dcats
    dcat_ids = [
        dcat['dcat_id'] for dcat in response['dcats']
    ]

    return dcat_ids

def get_event_results(event_id):
    dcat_ids = get_event_dcat_ids(event_id)
    
    responses = {'event_id': event_id}
    for dcat_id in dcat_ids:
        url = baseurl + f"/api/v1/events/{event_id}/result/{dcat_id}"
        results = requests.get(url, headers=headers).json()
        results['dcat_id'] = dcat_id
        responses['results'] = results
    
    return responses

def fetch_data():
    event_ids = get_event_ids()
    
    all_event_results = []
    for event_id in event_ids:
        event_results = get_event_results(event_id)
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
    
    return df

def transform_data(df):
    df = df.loc[df['dcat'].str.contains('boulder', case=False)]
    
    df['comp_id'] = df['event_id'].astype(str) + df['dcat_id'].astype(str)
    
    athletes_in_final = df[df['round']=='Final'][['comp_id', 'athlete_id']].drop_duplicates()
    df = df.merge(athletes_in_final, on=['comp_id', 'athlete_id'], how='inner')
    
    df = df.drop('status', axis=1)
    
    return df
