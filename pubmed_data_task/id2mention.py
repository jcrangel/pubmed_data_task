
"creates the dictionary id2mention from the dictionary term2entities_freq.pickle"

import pickle5 as pickle
from tqdm import tqdm



handle = open('term2entities_freq.pickle', 'rb')
mention2id = pickle.load(handle)


id2mention = {}
pbar = tqdm(total=len(mention2id))
for mention, v in mention2id.items():
    for i, id in enumerate(v['entities']):
        #is the first time we se the id
        mention2freq = {}
        if id not in id2mention:
            mention2freq[mention] = v['num_entity'][i]
            id2mention[id] = mention2freq
        else:
            if mention not in id2mention[id]:
                mention2freq[mention] = v['num_entity'][i]
                id2mention[id] = mention2freq

    pbar.update(1)

    # print(entities)
with open('id2mention.pickle', 'wb') as handle:
    pickle.dump(id2mention, handle, protocol=pickle.HIGHEST_PROTOCOL)
