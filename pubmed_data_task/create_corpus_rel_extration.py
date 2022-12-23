

from typing import List
import pickle5 as pickle
from tqdm import tqdm
import os
import numpy as np
import config

import networkx
import sys
from dkoulinker.entity_linker import load_ontology_entity_linker
from collections import namedtuple
import obonet

Example = namedtuple('Example',
                     ['entity_1', 'entity_2', 'left', 'mention_1', 'middle','mention_2', 'right'])

# Example = namedtuple('Example',
#                      'entity_1, entity_2, left, mention_1, middle, mention_2, right, '
def get_entities_pairs(onto_link,corpus:List[str],max_lines=300000):

    linked_corpus = []
    print('Linking entities of corpus')
    pbar = tqdm(total=max_lines)
    for n,line in enumerate(corpus):
        if n > max_lines:
            break
        mentions = onto_link.link_entities(line)
        if check_pair(mentions):
            last_end_pos=0
            i=0
            for mention in mentions:
                if not check_string_mask('CHEBI',mention):
                    continue
                if i==0:
                    entity_1 = mention['best_entity'][0]
                    mention_1 = mention['text']
                    left = line[:mention['start_pos']]
                    last_end_pos = mention['end_pos']
                elif i==1:
                    entity_2 = mention['best_entity'][0]
                    mention_2 = mention['text']
                    middle = line[last_end_pos:mention['start_pos']]
                    #Skip when theres no much context beetwenn the mentions
                    if len(middle.split(' ')) < 3 or len(middle) < 3 :
                        continue
                    right = line[mention['end_pos']:]
                i+=1
                pbar.update(1)

            if i>=2:
                linked_corpus.append(
                    Example(entity_1,entity_2,left,mention_1,middle,mention_2,right))
    
    return linked_corpus
def check_string_mask(mask,mention):
    return mention['best_entity'][0].split(':')[0] == mask
    
def check_pair(mentions):
    onto_mask="CHEBI"

    count_mask= 0
    for mention in mentions:
        if check_string_mask(onto_mask,mention):
            count_mask += 1
    
    return count_mask >= 2



url = '/home/julio/repos/dkouqe/data/ontologies/obo_files/chebi/chebi.obo'
graph = obonet.read_obo(url)  # Number of nodes
# print(len(graph))
# graph.number_of_edges()

onto_defs = [data.get('def').replace('"', '').replace('[]', '').replace('.', '')
             for _, data in graph.nodes(data=True) if data.get('def') is not None]


onto_link = load_ontology_entity_linker()

                                                        #DEBUG
linked_corpus = get_entities_pairs(onto_link,onto_defs)


print('loading pmid2title')
handle = open(config.PMID2DETAILS, 'rb')
pmid2title = pickle.load(handle)

lines_pubmed= [data['title'] for pmid,data in pmid2title.items()]
                                                               #debug      
linked_corpus_pubmed = get_entities_pairs(onto_link, lines_pubmed)


with open(os.path.sep.join(['linked_corpus.pickle']), 'wb') as handle:
    pickle.dump(linked_corpus+linked_corpus_pubmed, handle, protocol=pickle.HIGHEST_PROTOCOL)



