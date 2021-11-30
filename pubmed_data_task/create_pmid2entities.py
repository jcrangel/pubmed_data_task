""" Creates the file for calculating 
    and indexing the number of times an entity occurs in a pubmed article. 

    doc_entity_count[PMID][ENTITY_ID] = #count
    PMID
    ENTITY_ID : INT
"""

import pickle
import os
import pandas as pd
import json
import pprint
import time
from tqdm import tqdm
import bz2
import numpy as np

def create_entities_count():
    doc_entity_count = {}
    #File is huge work on chunkers
    pbar = tqdm(total=330394594)
    i_chunk = 0
    for df in pd.read_csv('../data/Bio_entities_small.csv', iterator=True, chunksize=1000000):
    # for df in pd.read_csv('../data/Bio_entities_small.csv', iterator=True,\
    #                         usecols=['Start', 'End', 'EntityID', 'PMID']):
        #iter current chunck
        start_time = time.time()
        for _, row in df.iterrows():
            pbar.update(1)
            # print(row)
            Pmid = int(row.PMID)
            EntityID = int(row.EntityID)
            Start = int(row.Start)
            End = int(row.End)
            #ignore mentions with no entityID
            if EntityID == 0 or EntityID =='0':
                continue
            #new pmid        
            if Pmid not in doc_entity_count:    
                #init mention
                
                # doc_entity_count[Pmid] = {EntityID: [[Start],[End]] }
                # doc_entity_count[Pmid] = np.array(
                #     [ list([EntityID]), list([[Start]]), list([End-Start]) ], dtype=object)
                doc_entity_count[Pmid] =  [ [EntityID], [[Start]], [End-Start] ]

                continue
            #
            # PMID exits in catalog but the entityID found is new    
            elif EntityID not in doc_entity_count[Pmid][0]:
                
                doc_entity_count[Pmid][0].append(EntityID)
                
                doc_entity_count[Pmid][1].append([Start])
                doc_entity_count[Pmid][2].append(End-Start)

                # doc_entity_count[Pmid][EntityID]['count'] = 1
                # doc_entity_count[Pmid][EntityID] = [[Start], [End]]
            #If entityID exist    
            else:#Increase counter
                # doc_entity_count[Pmid][EntityID]['count'] += 1
                # doc_entity_count[Pmid][EntityID][0].append(Start)
                # doc_entity_count[Pmid][0] = np.append(doc_entity_count[Pmid][0],[Start])
                # doc_entity_count[Pmid][1] = np.append(doc_entity_count[Pmid][1],[Start])
                # doc_entity_count[Pmid][EntityID][1].append(End)
                try:
                    #If the entityID exist increase the counter
                    index_entity = doc_entity_count[Pmid][0].index(EntityID)
                    doc_entity_count[Pmid][1][index_entity].append(Start)
                except ValueError:
                    pass

        #leave on firt chuck
        # break
        print("Chunk: {} took {} seconds to process".format(
            i_chunk, time.time()-start_time))
        i_chunk += 1

    for pmid,l in doc_entity_count.items():
        main_l = doc_entity_count[pmid] 
        ent_l = main_l[0]
        start_l = main_l[1]
        size_l = main_l[2]
        
        ent_t = tuple(ent_l)
        start_t = tuple(tuple(starts) for starts in start_l)
        size_t = tuple(size_l)
        main_t = (ent_t, start_t, size_t)
        # main_t = tuple(main_t)

        doc_entity_count[pmid] = main_t

        



    return doc_entity_count


doc_entity_count = create_entities_count()

# pprint.pprint(mentions)
# Store data (serialize)

with open(os.path.sep.join(['pmid2entities_small.pickle']), 'wb') as handle:
    pickle.dump(doc_entity_count, handle, protocol=pickle.HIGHEST_PROTOCOL)

# handle = bz2.BZ2File('pmid2entities_small.bz2', 'w')
# pickle.dump(doc_entity_count, handle,
#                 protocol = pickle.HIGHEST_PROTOCOL)
# Read pickle file with pickle 5
#import pickle5 as pickle
