""" Reads the entity descriptions from the file id2line.json, and saved it as a dictionary
of entities an descriptions.

dict[4167203] = 
['needed', 'determine', 'impact', 'hyperchloremia', 'chloride', 'ill', 'children', 'impact', 'chloride', 'load']

"""
from tqdm import tqdm
import re
import os
import string
from utils import cleanhtml
import pickle5 as pickle
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

DATASET_DIR = 'dataset'

file_desc = open(os.path.sep.join([DATASET_DIR, "id2line.json"]), 'r', encoding='utf8')
entities = {}

#number of lines
num_lines = 330271011
# num_lines = 10
# wc - l  dataset/id2line.json
#discard the first line is just an '{'
line = file_desc.readline()
count = 1

stop_words = set(stopwords.words("english"))

pbar = tqdm(total=num_lines)
while True:
    count += 1

    #last line
    if count == num_lines:
        break
    # if line is empty
    line = file_desc.readline()
    # end of file is reached
    if not line:
        break

    # Get next line from file
    # ' ": " ' is less likely to occurs than just ':'
    parts = line.split('": "')[1].split('\\t')
    try:
        #Remove <> <> tags
        desc = cleanhtml(parts[3]).replace('"', '').replace('\n','') 
        #Remove puntuation
        desc = desc.translate(str.maketrans('', '', string.punctuation))
        tokens = word_tokenize(desc)
        filtered_desc =[]
        for w in tokens:
            #do not take stop words
            if w not in stop_words:
                filtered_desc.append(w)

        entityID = parts[0].strip().replace('"','')
        mention = parts[2]
    except :
        print("Problem with line number",count)
        continue       
        
    # print(entityID)
    # print(mention)
    # print(desc)

    #Out of dictionary
    if entityID == 0 or entityID == '0':
        continue

    entities[int(entityID)] = filtered_desc
    pbar.update(1)
    # print(entityID,filtered_desc)
    # break


# print(entities)
with open(os.path.sep.join(['data_scoring', 'entityId2description.pickle']), 'wb') as handle:
    pickle.dump(entities, handle, protocol=pickle.HIGHEST_PROTOCOL)

