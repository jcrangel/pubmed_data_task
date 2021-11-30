""" Creates a dictionaty of term(words) from the entity catalog descriptions.
    Each item of the dictionary contains how many time the term appears in the
    whole collection. 

    terms_freq['pato'] = 1111
"""
import string
import os
import pickle5 as pickle
from nltk.tokenize import sent_tokenize, word_tokenize
from tqdm import tqdm

DATASET_DIR = 'data_scoring'

# file_desc = open(os.path.sep.join([DATASET_DIR, "id2line.json"]), 'r', encoding='utf8')
terms_freq = {}


print('Loading entity description dictionary ...')
handle_desc = open(os.path.sep.join([DATASET_DIR,'entityId2description.pickle']), 'rb')
entity_catalog = pickle.load(handle_desc)
print('Number of entities: ', len(entity_catalog))

pbar = tqdm(total=len(entity_catalog))
size_collections_desc = 0
for entity,desc in entity_catalog.items():
    # desc = desc.translate(str.maketrans('', '', string.punctuation))
    size_collections_desc += len(desc)
    for term in desc:
        if term not in terms_freq:
            terms_freq[term] = 1
        else: 
            terms_freq[term] += 1
    # print(entity,desc)
    # print(len(desc))
    pbar.update(1)

print(size_collections_desc)  # 5925118
with open(os.path.sep.join(['data_scoring', 'term2frequency.pickle']), 'wb') as handle:
    pickle.dump(terms_freq, handle, protocol=pickle.HIGHEST_PROTOCOL)

