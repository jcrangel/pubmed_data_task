"""
Converting pmid2entities internal list structurre to typles to save memory
"""

import pickle5 as pickle
from tqdm import tqdm
import os
import numpy as np 

handle = open('pmid2entities.pickle','rb')
pmid2ent = pickle.load(handle)
pbar = tqdm(total=len(pmid2ent))

for pmid, l in pmid2ent.items():
    pbar.update(1)
    main_l = pmid2ent[pmid]
    ent_l = main_l[0]
    start_l = main_l[1]
    size_l = main_l[2]

    ent_t = tuple(ent_l)
    start_t = tuple(tuple(starts) for starts in start_l)
    size_t = tuple(size_l)
    main_t = (ent_t, start_t, size_t)
    # main_t = tuple(main_t)

    pmid2ent[pmid] = main_t

with open(os.path.sep.join(['pmid2entities_np.pickle']), 'wb') as handle:
    pickle.dump(pmid2ent, handle, protocol=pickle.HIGHEST_PROTOCOL)
