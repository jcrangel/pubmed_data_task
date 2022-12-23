"""
Combine pmid files into a single one with the pmid info
"""

import pickle5 as pickle
from tqdm import tqdm
import os
import numpy as np
import config


print('loading pmid2entities')
handle = open(config.PMID2ENTITIES, 'rb')
pmid2entities = pickle.load(handle)
print('loading pmid2title')
handle = open(config.PMID2DETAILS, 'rb')
pmid2title = pickle.load(handle)

# print('loading pmid2abstract_morethan10')
# pmids = np.load('../faiss_search/dumps/pmids_abstract_morethan10.npy')

pbar = tqdm(total=len(pmid2title))
# MAX_ABS_LEN = 300
pmid2info = {}
for pmid,data in pmid2title.items():
    pbar.update(1)
    pmid = int(pmid)
    title = data['title']
    abstract = data['abstract']
    journal = data['journal']
    pubdate = data['pubdate']
    #I think pmid2entities only has pmids from documents that contais entitiesIDs with ID diferent than zero. 
    if pmid in pmid2entities:
        entities = pmid2entities[pmid][0]
        starts = pmid2entities[pmid][1]
        sizes = pmid2entities[pmid][2]
    else:
        entities = set()
        starts = set()
        sizes = set()

    # info = (entities,starts,sizes,title,abstract[:MAX_ABS_LEN])
    info = (entities,starts,sizes,title,abstract,journal,pubdate)

    pmid2info[pmid] = info

with open(os.path.sep.join(['pmid2info_full.pickle']), 'wb') as handle:
    pickle.dump(pmid2info, handle, protocol=pickle.HIGHEST_PROTOCOL)


