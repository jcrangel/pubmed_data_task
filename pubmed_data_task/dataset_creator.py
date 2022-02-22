
import pubmed_parser as pp
from tqdm import tqdm

import glob


def create_pmid2details(
    data_path,
    fields = ['title','abstract','journal','pubdate']
    ):
    """Extracts textual information for the raw files of pubmed. 
    Create a dictionary that maps pmid to articles 'title', 'abstract', 'journal'

    :param data_path: Data path to *.xml.gz files
    :type data_path: [type]
    :param batch_size: [description], defaults to 2
    :type batch_size: int, optional
    """


    # files = glob.glob("../data/*.xml.gz")
    files = glob.glob(data_path+"/*.xml.gz")


    num_files = len(files)
    pbar = tqdm(total=num_files)
    pmid2details = {}
    # since disk IO is the bottle neck does batches are useful here?
    # num_chunks = int(num_files / batch_size)
    # nchunk = 0
    #iterate on all xml.gz file
    # for i in range(0, num_files, batch_size):
    for file in files:
        # nchunk += 1
        # Read all the xml files from the *.gz
        #For each xml now the have a list of dict
        #Each dict:
        #{'title': 'Hereditary spherocytosis ',
        # 'abstract': 'aaaabbbbssssttttrrrraaaccc',
        # 'journal': 'The Journal of the Association of Physicians of India',
        # 'authors': 'P Satheesh;K Pavithran;M Thomas', 'pubdate': '1994', 'pmid': '7860526',..}
        # for article in articles:

         #this a list of dictionaries, each dictionary is an article
        _articles = pp.parse_medline_xml(file)
        # https: // stackoverflow.com/questions/5352546/extract-subset-of-key-value-pairs-from-python-dictionary-object
        for article in _articles:
            pmid2details[int(article['pmid'])] = {k:article[k] for k in fields}

        pbar.update(1)

    
    print("Done indexing.")

    return pmid2details


def create_pmid2info():
    import pubmed_data_task.config as config
    import numpy as np
    import pickle 
    print('loading pmid2entities')
    handle = open(config.PMID2ENTITIES, 'rb')
    pmid2entities = pickle.load(handle)
    print('loading pmid2details')
    handle = open(config.PMID2DETAILS, 'rb')
    pmid2details = pickle.load(handle)

    print('loading pmid2abstract_morethan10')
    pmids = np.load(config.PMIDS_SIZEABS_MORE10)

    pbar = tqdm(total=len(pmids))
    # MAX_ABS_LEN = 300
    pmid2info = {}
    for pmid in pmids:
        pbar.update(1)
        pmid = int(pmid)
        title = pmid2details[pmid]['title']
        abstract = pmid2details[pmid]['abstract']
        journal = pmid2details[pmid]['journal']
        pubdate = pmid2details[pmid]['pubdate']
        #I think pmid2entities only has pmids from documents that contais entitiesIDs with ID diferent than zero.
        if pmid not in pmid2entities:
            continue
        entities = pmid2entities[pmid][0]
        starts = pmid2entities[pmid][1]
        sizes = pmid2entities[pmid][2]
        # info = (entities,starts,sizes,title,abstract[:MAX_ABS_LEN])
        info = (entities, starts, sizes, title, abstract,journal,pubdate)

        pmid2info[pmid] = info

    return pmid2info


def create_pmid2details_ontologies(
    data_path,
    e_linker,
    fields=['title', 'abstract']
):
    """Link the entities from the text 

    :param data_path: Data path to *.xml.gz files
    :type data_path: [type]
    :param batch_size: [description], defaults to 2
    :type batch_size: int, optional
    """

    

    # files = glob.glob("../data/*.xml.gz")
    files = glob.glob(data_path+"/*.xml.gz")
    #DEBUG
    files = [data_path+'/pubmed21n0001.xml.gz']
    num_files = len(files)
    pbar = tqdm(total=num_files)
    pmid2details = {}

    for file in files:
        # nchunk += 1
        # Read all the xml files from the *.gz
        #For each xml now the have a list of dict
        #Each dict:
        #{'title': 'Hereditary spherocytosis ',
        # 'abstract': 'aaaabbbbssssttttrrrraaaccc',
        # 'journal': 'The Journal of the Association of Physicians of India',
        # 'authors': 'P Satheesh;K Pavithran;M Thomas', 'pubdate': '1994', 'pmid': '7860526',..}
        # for article in articles:

        #this a list of dictionaries, each dictionary is an article
        print('Parsing file:',file)
        _articles = pp.parse_medline_xml(file)
        print('[FINISHED]Parsing file:',file)
        # https: // stackoverflow.com/questions/5352546/extract-subset-of-key-value-pairs-from-python-dictionary-object
        pbar2 = tqdm(total=len(_articles))
        texts = []
        for article in _articles:
            # pmid2details[int(article['pmid'])] = {
            #     k: article[k] for k in fields}
            pmid = int(article['pmid'])
            title = article['title'] 
            abstract = article['abstract']
            text = title+abstract
            # print('Linking article:',pmid)

            spans = e_linker.link_entities(text)
            # print('[FINISHED] Linking article:',pmid)
            
            for span in spans:
                #Each span have one entity info
                start = span['start_pos']
                size = len(span['text'])
                ent_id = span['best_entity'][0]
                if pmid not in pmid2details:
                    pmid2details[pmid] = [
                        [ ent_id ],
                        [ [start] ],
                        [ size ],
                        title,abstract ]
                else:
                    #TODO add try catch..
                    try:
                        pmid2details[pmid][0].append(ent_id)
                        pmid2details[pmid][1].append([start])
                        pmid2details[pmid][2].append(size)
                    except AttributeError:
                        continue
                
                #All to tuples 
            if len(spans) > 0:
                pmid2details[pmid][0] = tuple(pmid2details[pmid][0])
                pmid2details[pmid][1] = tuple([tuple(l) for l in pmid2details[pmid][1]])
                pmid2details[pmid][2] = tuple(pmid2details[pmid][2])
                pmid2details[pmid] = tuple(pmid2details[pmid])
                #debug
                # break
            pbar2.update(1)
        #  30941:
        # (
        # (entityid1,              entityid2, ..),
        #  ((start1, start2, start3),  (start1, start2, start3), ..),
        #  (size1,                   size2, ..),
        #  'title',
        #  'abstract',
        #  'journal inf',
        #  'year info',

        #  )
        
        pbar.update(1)
    print(pmid2details)
    print("Done indexing.")

    return pmid2details


def load_EL_onto():
    import pickle5 as pickle
    import time
    from dkoulinker.entity_linker import EntityLinker, get_mentions_ner
    from flair.models import SequenceTagger
    from dkoulinker.entity_ranking import DictionaryRanking, QueryEntityRanking
    from dkoulinker.utils import _print_colorful_text
    import dkoulinker.config as data_path
        #loading dicitonary of commonness,
    print('Loading mention2pem dictionary ...')
    handle = open(data_path.ONTO_PEM, 'rb')
    mention2pem = pickle.load(handle)


    print('Loading entity description dictionary ...')
    handle_desc = open(data_path.ONTO_ENTITY2DESCRIPTION, 'rb')
    entity2description = pickle.load(handle_desc)
    print('NUmber of entities: ', len(entity2description))

    print('Loading dictionary of term frequency ...')
    handle_desc = open(data_path.ONTO_MENTION_FREQ, 'rb')
    mention2freq = pickle.load(handle_desc)
    print('Number of term in the collection: ', len(mention2freq))

    #given by create_term_req
    collection_size_terms = len(mention2pem)

    # load the NER tagger
    tagger = SequenceTagger.load(data_path.ONTO_TAGGER)


    dictionarysearch_strategy = DictionaryRanking(mention2pem)
    queryranking_strategy = QueryEntityRanking(
        entity2description=entity2description,
        mention_freq=mention2freq,
        mention2pem=mention2pem,
        p_t_thetae_method='bayesian'  # Smoothing method
    )
    e_linker = EntityLinker(
        ranking_strategy=queryranking_strategy,
        entity2description=entity2description,
        ner_model=tagger,
        mention2pem=mention2pem,
        prune_overlapping_method='large_text',
        use_ner_dict=True

    )

    return e_linker
