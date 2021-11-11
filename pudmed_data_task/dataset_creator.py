
import pubmed_parser as pp
from tqdm import tqdm

import glob
import os
import time

def create_pmid2details(
    data_path,
    batch_size=2,
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


