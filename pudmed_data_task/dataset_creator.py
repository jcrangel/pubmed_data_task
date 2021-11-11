import pubmed_parser as pp
from tqdm import tqdm
import pickle
import glob
import os
import time

def create_pmid2details(data_path,batch_size=2):
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
    # pbar = tqdm(total=num_files)

    num_chunks = int(num_files / batch_size)
    nchunk = 0
    #iterate on all xml.gz file
    for i in range(0, num_files, batch_size):
        nchunk += 1
        # Read all the xml files from the *.gz
        #For each xml now the have a list of dict
        #Each dict:
        #{'title': 'Hereditary spherocytosis ',
        # 'abstract': 'aaaabbbbssssttttrrrraaaccc',
        # 'journal': 'The Journal of the Association of Physicians of India',
        # 'authors': 'P Satheesh;K Pavithran;M Thomas', 'pubdate': '1994', 'pmid': '7860526',..}
        # for article in articles:
        chunk_files = files[i:i+batch_size]
        print("Processing chunk {}/{}".format(nchunk, num_chunks))
        articles = []
        print("Parsing chunk of files:..")
        start_parsing = time.time()
        for file in chunk_files:
            #this a list of dictionaries, each dictionary is an article
            __articles = pp.parse_medline_xml(file)
            # https: // stackoverflow.com/questions/5352546/extract-subset-of-key-value-pairs-from-python-dictionary-object
            for _article in __articles:
                #grab only the keys we want
                article = {k: _article.get(k, ' ')
                           for k in ('title', 'abstract', 'pmid')}
                articles.append(article)
        # index_batch(articles[-100:]) #debug TODO
        print("Parsing chunk took:{} seconds".format(time.time()-start_parsing))
        print("Indexing chunk..")
        index_batch(articles)
        print("Process chunk took:{} seconds".format(time.time()-start_parsing))
        #process it in batchs

        # break  # debug TODO
        # pbar.update(1)

    es.indices.refresh(index=ARTICLE_INDEX_NAME)
    print("Done indexing.")

    return
