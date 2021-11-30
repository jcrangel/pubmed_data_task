"""
Combine pmid files into a single one with the pmid info
"""

import pickle5 as pickle
from tqdm import tqdm
import os
import numpy as np

print('loading pmid2entities')
handle = open('pmid2entities.pickle', 'rb')
pmid2entities = pickle.load(handle)
print('loading pmid2title')
handle = open('../faiss_search/dumps/pmid2title.pickle', 'rb')
pmid2title = pickle.load(handle)

print('loading pmid2abstract_morethan10')
pmids = np.load('../faiss_search/dumps/pmids_abstract_morethan10.npy')

pbar = tqdm(total=len(pmids))
# MAX_ABS_LEN = 300
pmid2info = {}
for pmid in pmids:
    pbar.update(1)
    pmid = int(pmid)
    title = pmid2title[pmid]['title']
    abstract = pmid2title[pmid]['abstract']
    #I think pmid2entities only has pmids from documents that contais entitiesIDs with ID diferent than zero. 
    if pmid not in pmid2entities:
        continue
    entities = pmid2entities[pmid][0]
    starts = pmid2entities[pmid][1]
    sizes = pmid2entities[pmid][2]
    # info = (entities,starts,sizes,title,abstract[:MAX_ABS_LEN])
    info = (entities,starts,sizes,title,abstract)

    pmid2info[pmid] = info

with open(os.path.sep.join(['pmid2info_full.pickle']), 'wb') as handle:
    pickle.dump(pmid2info, handle, protocol=pickle.HIGHEST_PROTOCOL)


