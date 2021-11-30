""" Creates the file for calculating the commoness of a entity
    The surface forms of the entities 
    mentions[term] = {"entities": [EntityID1,entityID2], "num_entity": [1,4], "num": 5}
"""

import pickle
import os
import pandas as pd
import json
import pprint
import time
from tqdm import tqdm

def create_mentions_commonness():
    mentions = {}
    #File is huge work on chunkers 
    pbar = tqdm(total=330394594)
    i_chunk = 0
    for df in pd.read_csv('data/Bio_entities_Main.csv', iterator=True, chunksize=100000):    
        #iter current chunck
        start_time = time.time()
        for index,row in df.iterrows():
            pbar.update(1)    
            # print(row)
            #TODO Change all to int, str take a lot more of memory
            Mention = int(row.Mention)
            EntityID = int(row.EntityID)
            #ignore mentions with no entityID
            if EntityID == 0:
                continue 
            if Mention not in mentions:
                #init mention
                mentions[Mention] = {"entities": [EntityID], "num_entity": [1], "num": 1}
                continue

            #check entityID 
            try:
                #If the entityID exist increase the counter
                index_entity = mentions[Mention]["entities"].index(EntityID)
                mentions[Mention]["num_entity"][index_entity] += 1
            except ValueError:
                #EntityID is new
                # print("New entity:",EntityID,"for mention:",Mention)
                mentions[Mention]["entities"].append(EntityID)
                mentions[Mention]["num_entity"].append(1)

            mentions[Mention]["num"] += 1
        
        #leave on firt chuck
        # break
        print("Chunk: {} took {} seconds to process".format(i_chunk,time.time()-start_time))
        i_chunk += 1

    
    return mentions


mentions = create_mentions_commonness()

# pprint.pprint(mentions)
# Store data (serialize)

with open(os.path.sep.join(['data_scoring','term2entities_freq.pickle']), 'wb') as handle:
    pickle.dump(mentions, handle, protocol=pickle.HIGHEST_PROTOCOL)
# Read pickle file with pickle 5
#import pickle5 as pickle

# from pickle5 import pickle
# handle = open('data_scoring/mentions_commonness.pickle', 'rb')
# dict = pickle.load(handle)
