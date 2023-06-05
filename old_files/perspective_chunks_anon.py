import os
import numpy as np
import pandas as pd
import time
import json
import multiprocessing
from matplotlib import pyplot as plt
from googleapiclient import discovery
from googleapiclient.errors import HttpError
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from perspective import PerspectiveAPI



p1 = PerspectiveAPI("xxxxx") #200
p2 = PerspectiveAPI("xxxxx") #100


def query_perspective_slim_p1(text):#, ordinal, api):
    try :
        result = p1.analyze_comment(text=text, requested_attributes={"TOXICITY": {}, 'THREAT': {}, 'SEVERE_TOXICITY': {}, 'IDENTITY_ATTACK': {}, 'PROFANITY': {}, 'INSULT': {}})
        result = result.get('attributeScores')
    except (Exception,HttpError) as err:
        if (type(err) == HttpError):
            if (err.status_code == 429):
                print('Going to slepop bye bye')
                time.sleep(61)
                return query_perspective_slim_p1(api = p1, text=text)
            elif(err.status_code == 400):
                return [None] * 6
        else :
            return [None] * 6
    return [result.get('TOXICITY').get('summaryScore').get('value'), 
            result.get('SEVERE_TOXICITY').get('summaryScore').get('value'),
            result.get('THREAT').get('summaryScore').get('value'),
            result.get('IDENTITY_ATTACK').get('summaryScore').get('value'),
            result.get('PROFANITY').get('summaryScore').get('value'),
            result.get('INSULT').get('summaryScore').get('value')]
    
    
def query_perspective_slim_p2(text):#, ordinal, api):
    try :
        result = p2.analyze_comment(text=text, requested_attributes={"TOXICITY": {}, 'THREAT': {}, 'SEVERE_TOXICITY': {}, 'IDENTITY_ATTACK': {}, 'PROFANITY': {}, 'INSULT': {}})
        result = result.get('attributeScores')
    except (Exception,HttpError) as err:
        if (type(err) == HttpError):
            if (err.status_code == 429):
                print('Going to slepop bye bye')
                time.sleep(61)
                return query_perspective_slim_p2(api = p2, text=text)
            elif(err.status_code == 400):
                return [None] * 6
        else :
            return [None] * 6
    return [result.get('TOXICITY').get('summaryScore').get('value'), 
            result.get('SEVERE_TOXICITY').get('summaryScore').get('value'),
            result.get('THREAT').get('summaryScore').get('value'),
            result.get('IDENTITY_ATTACK').get('summaryScore').get('value'),
            result.get('PROFANITY').get('summaryScore').get('value'),
            result.get('INSULT').get('summaryScore').get('value')]
    
    
    
def perspective_api1(csv):
    input_folder = 'C:/Users/CreCre/Downloads/youtube-crosstalk/data/comments'
    output_folder = "D:/perspective5_c"
    visited_video_set = set()
    
    if os.path.exists(output_folder):
        existing = os.listdir(output_folder)
        for f in existing:
            mk1 = f.find('_') + 1
            mk2 = f.find('.', mk1)
            substring = f[ mk1 : mk2 ]
            visited_video_set.add(substring)
            
    mk1_2 = csv.find('_') + 1
    mk2_2 = csv.find('.', mk1_2)
    substring_2 = csv[ mk1_2 : mk2_2 ]
    video_id = substring_2
    
    if csv != '' and video_id not in visited_video_set: 
        try:
            #start = time.time()
            video = input_folder + '/' + csv     
            video_df = pd.read_csv(video)
            chunk_size = 100
            chunks = [video_df.iloc[video_df.index[i:i + chunk_size]] for i in range(0, video_df.shape[0], chunk_size)]
            resultone = []
            for chunk in chunks:
                with ThreadPoolExecutor(max_workers=100) as executor:
                    results = []
                    for result in executor.map(query_perspective_slim_p1, chunk['textOriginal']):
                        results.append(result)
                    resultone.append(results)
    
            flat_list = [item for sublist in resultone for item in sublist]
            video_df[['TOXICITY', 'THREAT', 'SEVERE_TOXICITY', 'IDENTITY_ATTACK', 'PROFANITY', 'INSULT']]=flat_list
            video_df.to_csv("D:/perspective5_c/perspective_{0}.csv". format(video_id), index=False)
            #print('Saved ', video_id, 'Len: ', video_df.shape[0], 'Time taken: ', (time.time() - start))
        except:
            #print('Problem with ', video) 
            None      
    else:
        #print(video_id, ': already visited') 
        None  
        
        
        
        
def perspective_api2(csv):
    input_folder = "C:/Users/CreCre/Downloads/youtube-crosstalk/data/comments"
    output_folder = "D:/perspective5_c"
    visited_video_set = set()

    if os.path.exists(output_folder):
        existing = os.listdir(output_folder)
        for f in existing:
            mk1 = f.find('_') + 1
            mk2 = f.find('.', mk1)
            substring = f[ mk1 : mk2 ]
            visited_video_set.add(substring)
    mk1_2 = csv.find('_') + 1
    mk2_2 = csv.find('.', mk1_2)
    substring_2 = csv[ mk1_2 : mk2_2 ]
    
    video_id = substring_2
    if csv != '' and video_id not in visited_video_set:
        try:
            #start = time.time()
            video = input_folder + '/' + csv     
            video_df = pd.read_csv(video)
            chunk_size = 100
            chunks = [video_df.iloc[video_df.index[i:i + chunk_size]] for i in range(0, video_df.shape[0], chunk_size)]
            resultone = []
            for chunk in chunks:
                with ThreadPoolExecutor(max_workers=100) as executor:
                    results = []
                    for result in executor.map(query_perspective_slim_p2, chunk['textOriginal']):
                        results.append(result)
                    resultone.append(results)
            
            flat_list = [item for sublist in resultone for item in sublist]
            video_df[['TOXICITY', 'THREAT', 'SEVERE_TOXICITY', 'IDENTITY_ATTACK', 'PROFANITY', 'INSULT']]=flat_list
            video_df.to_csv("D:/perspective5_c/perspective_{0}.csv". format(video_id), index=False)
            #print('Saved ', video_id, 'Len: ', video_df.shape[0], 'Time taken: ', (time.time() - start))
            
        except:
            #print('Problem with ', video)
            None
    else:
        #print(video_id, ': already visited') 
        None
        

            
def run_api(listarella):
    
    start = time.time()
    slice1 = listarella[0]
    slice2 = listarella[1]
    api = listarella[2]
    process_nr = listarella[3]
    #print(slice1, slice2, api)
    if api == 1:
        
        queried_counter = 0
        #print('api1 uee')
        for csv in os.listdir('C:/Users/CreCre/Downloads/youtube-crosstalk/data/comments')[slice1:slice2]:
            perspective_api1(csv)
            queried_counter += 1
            print('Process nr.', process_nr, 'queried', queried_counter, 'videos, time elapsed: ', time.time() - start, end="\r", flush=True)
        print('--- FINISHED process nr.', process_nr, 'queried', queried_counter, 'videos, time elapsed: ', time.time() - start) 
    elif api == 2:
        queried_counter = 0
        #print('api2 uee')
        for csv in os.listdir('C:/Users/CreCre/Downloads/youtube-crosstalk/data/comments')[slice1:slice2]:
            perspective_api2(csv)
            queried_counter += 1
            print('Process nr.', process_nr, 'queried', queried_counter, 'videos, time elapsed: ', time.time() - start, end="\r", flush=True)
        print('--- FINISHED process nr.', process_nr, 'queried', queried_counter, 'videos, time elapsed: ', time.time() - start)
            
            

    
   
slices = [
    [60000, 70000, 1, 1],
    [70000, 80000, 1, 2],
    [80000, 90000, 2, 3]
]

'''slices = [
    [15650, 16000, 1, 1],
    [16000, 23032, 1, 2],
    [15000, 15650, 2, 3]
]'''


def runner():
    with ProcessPoolExecutor(max_workers=3) as executor:
        #print('Uee guagliò')
        
        for lista in slices:
            #print(slice1, slice2, api)
            executor.submit(run_api, lista)
    print('Arrived till', slices[2][1])
        


if __name__ == '__main__':       
    runner()
    