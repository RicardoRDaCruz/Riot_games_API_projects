import os
import requests
from concurrent.futures import as_completed, ProcessPoolExecutor
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from dotenv import load_dotenv

#load_dotenv()
#api_key=os.getenv('API_KEY')
api_key='RGAPI-01e608ac-0304-4c9a-b7db-d6b53fcfea34'

ranked5x5='RANKED_SOLO_5x5'
gn='永遠の拷問'
tl='xox'

def get_puuid(summonerId=None, game_name=None, tag_line=None, region=None):

    session_summonerId = FuturesSession(executor=ProcessPoolExecutor(max_workers=10))
    session_gn_tn = FuturesSession(executor=ProcessPoolExecutor(max_workers=10))
    retries = 10
    status_forcelist = [500, 502, 503, 504, 429]
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        respect_retry_after_header=True,
        status_forcelist=status_forcelist,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session_summonerId.mount('http://', adapter)
    session_summonerId.mount('https://', adapter)
    
    session_gn_tn.mount('http://', adapter)
    session_gn_tn.mount('https://', adapter)
    
    if summonerId is not None:
        url_1=f'https://{region}.api.riotgames.com'
        url_2='/lol/summoner/v4/summoners/'
        url=f'{url_1}{url_2}{summonerId}?api_key={api_key}'
        future = session_summonerId.get(url)
        response = future.result()
        return response.json()['puuid']
    else:
        url_1=f'https://{region}.api.riotgames.com'
        url_2=f'/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}'
        url=f'{url_1}{url_2}?api_key={api_key}'
        future = session_gn_tn.get(url)
        response = future.result()   
        return response.json()['puuid']

def get_gn_tn(region=None, puuid=None):
    url_1=f'https://{region}.api.riotgames.com'
    url_2=f'/riot/account/v1/accounts/by-puuid/{puuid}'
    url=f'{url_1}{url_2}?api_key={api_key}'
    response=requests.get(url)
    id={
        'gameName':response.json()['gameName'],
        'tagLine':response.json()['tagLine']
    }
    return id

def get_queue(region=None, rank=None, mode=None):    
    url_1=f'https://{region}.api.riotgames.com'
    url_2=f'/lol/league/v4/{rank}leagues/by-queue/{mode}'
    url=f'{url_1}{url_2}?api_key={api_key}'
    response=requests.get(url)
    return response.json()

def get_top_rank(region, top=2000):
    queue_challenger=pd.DataFrame(get_queue(region,'challenger',ranked5x5)['entries']).sort_values('leaguePoints',ascending=False).reset_index(drop=True)
    queue_grandmaster=pd.DataFrame(get_queue(region,'grandmaster',ranked5x5)['entries']).sort_values('leaguePoints',ascending=False).reset_index(drop=True)
    queue_master=pd.DataFrame(get_queue(region,'master',ranked5x5)['entries']).sort_values('leaguePoints',ascending=False).reset_index(drop=True)
    queue_concat=pd.concat([queue_challenger,queue_grandmaster,queue_master]).reset_index(drop=True)
    queue_concat=queue_concat.drop(columns='rank').reset_index(drop=False).rename(columns={'index':'posicao'})
    queue_concat['posicao']+=+1
    return queue_concat[0:top]

def get_match_history(region=None, puuid=None, start=0, count=20):

    session = FuturesSession(executor=ProcessPoolExecutor(max_workers=10))
    retries = 10
    status_forcelist = [500, 502, 503, 504, 429]
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        respect_retry_after_header=True,
        status_forcelist=status_forcelist,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    url_1=f'https://{region}.api.riotgames.com'
    url_2=f'/lol/match/v5/matches/by-puuid/{puuid}/ids'
    query_params=f'?{start}=0&{count}=100'
    url=f'{url_1}{url_2}{query_params}&api_key={api_key}'
    future = session.get(url)
    response = future.result()
    return response.json()

def get_match_data_from_id(region=None, matchId=None):

    session = FuturesSession(executor=ProcessPoolExecutor(max_workers=10))
    retries = 10
    status_forcelist = [500, 502, 503, 504, 429]
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        respect_retry_after_header=True,
        status_forcelist=status_forcelist,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    
    url_1=f'https://{region}.api.riotgames.com'
    url_2=f'/lol/match/v5/matches/{matchId}'
    url=f'{url_1}{url_2}?api_key={api_key}'

    future = session.get(url)
    response=future.result()

    return response.json()  

def process_match_json_one_player(match_json, puuid):  

    #architecture
    metadata=match_json['metadata']    
    info=match_json['info']      
    players=info['participants']
    participants=metadata['participants']
    teams=info['teams']        
    player=players[participants.index(puuid)]
    team_id = player['teamId']
    if [teams[0]['teamId'],teams[1]['teamId']] == [100,0]:
        teams[1]['teamId'] = 200
    perks = player['perks']
    stats = perks['statPerks']
    styles = perks['styles']

    primary = styles[0]
    secondary = styles[1]

    side_dict = {
        100: 'blue',
        200: 'red',
    }

    match_id=metadata['matchId'] 

    game_creation = info['gameCreation']
    game_duration = info['gameDuration']
    game_end_timestamp = info['gameEndTimestamp']
    patch = info['gameVersion']

    riot_id = player['riotIdGameName']
    riot_tag = player['riotIdTagline']
    summoner_id = player['summonerId']
    summoner_name = player['summonerName']

    side = side_dict[player['teamId']]
    lane = player['lane']
    win = player['win']

    champ_id = player['championId'] 
    
    champ_name = retrieve_champ_name(champ_id)
    champ_transform = player['championTransform']
    champ_level = player['champLevel']

    team_position = player['teamPosition']

    gold_earned = player['goldEarned']
    neutral_minions_killed = player['neutralMinionsKilled']
    total_minions_killed = player['totalMinionsKilled']    

    kills = player['kills']
    deaths = player['deaths']
    assists = player['assists']
    first_blood = player['firstBloodKill']   

    total_damage_dealt = player['totalDamageDealtToChampions']
    total_damage_shielded = player['totalDamageShieldedOnTeammates']
    total_damage_taken = player['totalDamageTaken']
    total_damage_healed = player['totalHealsOnTeammates']
    
    early_surrender = player['gameEndedInEarlySurrender']
    surrender = player['gameEndedInSurrender']    

    item0 = player['item0']
    item1 = player['item1']
    item2 = player['item2']
    item3 = player['item3']
    item4 = player['item4']
    item5 = player['item5']
    item6 = player['item6']        
    
    summoner_1_id = player['summoner1Id']
    summoner_2_id = player['summoner2Id']    
    
    total_time_cc_dealt = player['totalTimeCCDealt']
    wards_placed = player['wardsPlaced']
    wards_killed = player['wardsKilled']
    vision_score =  player['visionScore']    

    objectives_stolen = player['objectivesStolen']
    objectives_stolen_assists = player['objectivesStolenAssists']

    defense = stats['defense']
    flex = stats['flex']
    offense = stats['offense']    

    primary_style = primary['style']
    secondary_style = secondary['style']

    primary_keystone = primary['selections'][0]['perk']
    primary_perk_1 = primary['selections'][1]['perk']
    primary_perk_2 = primary['selections'][2]['perk']
    primary_perk_3 = primary['selections'][3]['perk']

    secondary_perk_1 = secondary['selections'][0]['perk']
    secondary_perk_2 = secondary['selections'][1]['perk']
    
    #???
    detector_wards_placed = player['detectorWardsPlaced']
    role = player['role']
    print(role)
    vision_wards_bought = player['visionWardsBoughtInGame']
    
    for team in teams:
        if team['teamId']==player['teamId']:
            bans = team['bans']
            obj = team['objectives']
            baron = obj['baron']
            dragon = obj['dragon']
            rift_herald=obj['riftHerald']
            grubs = obj['horde']
            atackhan = obj['atakhan']
            tower = obj['tower']
            inhibitor = obj['inhibitor']

    team_objectives = [baron, dragon, rift_herald, grubs, atackhan, tower, inhibitor]
        
    for obj in [baron, dragon, rift_herald, grubs, atackhan, tower, inhibitor]:
            first=obj['first']
            obj_kills=obj['kills']

    matchDF = pd.DataFrame({
        'match_id': [match_id],
        'participants': [participants],
        'game_creation': [game_creation],
        'game_duration': [game_duration],
        'game_end_timestamp': [game_end_timestamp],
        'patch': [patch],
        'riot_id': [riot_id],
        'riot_tag': [riot_tag],
        'summoner_id': [summoner_id],
        'summoner_name': [summoner_name],
        'side': [side],
        'lane': [lane],
        'bans': [bans],
        'win': [win],
        'champ_name': [champ_name],
        'champ_transform': [champ_transform],
        'champ_level': [champ_level],
        'team_position': [team_position],
        'gold_earned': [gold_earned],
        'neutral_minions_killed': [neutral_minions_killed],
        'total_minions_killed': [total_minions_killed],
        'kills': [kills],
        'deaths': [deaths],
        'assists': [assists],
        'team_objetives': [team_objectives],
        'first_blood': [first_blood],
        'total_damage_dealt': [total_damage_dealt],
        'total_damage_shielded': [total_damage_shielded],
        'total_damage_taken': [total_damage_taken],
        'total_damage_healed': [total_damage_healed],        
        'early_surrender': [early_surrender],
        'surrender': [surrender],
        'item0': [item0],
        'item1': [item1],
        'item2': [item2],
        'item3': [item3],
        'item4': [item4],
        'item5': [item5],
        'item6': [item6],        
        'summoner_1_id': [summoner_1_id],
        'summoner_2_id': [summoner_2_id],        
        'total_time_cc_dealt': [total_time_cc_dealt],
        'wards_placed': [wards_placed],
        'wards_killed': [wards_killed],
        'vision_score': [vision_score],
        'objectives_stolen': [objectives_stolen],
        'objectives_stolen_assists': [objectives_stolen_assists],               
        'primary_keystone': [primary_keystone],
        'perk_primary_row_1': [primary_perk_1],
        'perk_primary_row_2': [primary_perk_2],
        'perk_primary_row_3': [primary_perk_3],
        'perk_secondary_row_1': [secondary_perk_1],
        'perk_secondary_row_2': [secondary_perk_2],
        'primary_style': [primary_style],
        'secondary_style': [secondary_style],
        'perk_shard_defense': [defense],
        'perk_shard_flex': [flex],
        'perk_shard_offense': [offense], 
    })

    return matchDF

def build_match_df(region, playersDF, start, count):
    df=pd.DataFrame()
    for player in playersDF.itertuples(index=False):
        game_history=get_match_history(region=region, puuid=player.puuid,start=start,count=count)
        for game in game_history:
            game_json = get_match_data_from_id(region, matchId=game)
            df=pd.concat([df, process_match_json_one_player(game_json, player.puuid)])
    return df

def get_community_dragon_json(element):
    url=f'https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/default/v1/{element}.json'
    response=requests.get(url)
    json=response.json()
    return json

def json_extract(obj, key):
    arr = []
    def extract(obj, arr, key):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k==key:
                    arr.append(v)
                elif isinstance(v, (dict, list)):
                    extract(v, arr, key)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr    
    values = extract(obj, arr, key)
    return values

def build_ids_names_dict(json):
    ids = json_extract(json, 'id')
    names = json_extract(json, 'name')
    dict_ids_names = dict(map(lambda i, j : (int(i), j), ids, names))
    return dict_ids_names
        
def retrieve_champ_name(champ_id):
    url = f'https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/default/v1/champions/{str(champ_id)}.json'
    response = requests.get(url)
    champ_json = response.json()
    champ_name = json_extract(champ_json, 'name')[0]
    return champ_name

def main():
    #puuid=get_puuid(region='europe',game_name=gn,tag_line=tl)
    #gn_tl=get_gn_tn(region='europe', puuid=puuid)
    #br_2000=get_top_rank('br1',2000)
    #match_histories=[]
    #for i in br_2000.head()['puuid']:
    #    match_histories.append(get_match_history(region='americas',puuid=i))
    br_1 = get_top_rank('br1',1)    
    df_1 = build_match_df('americas', br_1,0,count=1)
    #kr_1 = get_top_rank('kr',5)    
    #df_2 = build_match_df(region='asia', playersDF=kr_1,start=0,count=10)
    #print(df_1['game_duration'].mean())
    #print(df_2['game_duration'].mean())

    community_dragon_list = ['items','perks','perkstyles']
    community_dragon_dict = {}
    for element in community_dragon_list:
        element_json = get_community_dragon_json(element)
        element_dict=build_ids_names_dict(element_json)
        community_dragon_dict[element] = element_dict

    for element in community_dragon_dict:
        df_1 = df_1.replace(community_dragon_dict[element])  

if __name__ == "__main__":
    main()
