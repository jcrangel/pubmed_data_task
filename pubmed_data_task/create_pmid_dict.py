"""
Create a dictionary for files and pmids. 
It reads all pubmed raw xml.gz and create a dict s.t:
pmid2file['3092'] = 'pubmed21n0001.xml.gz'
pmid2file.pickle is the saved dictionary.
"""
from tqdm import tqdm
import pubmed_parser as pp
import glob
import os
import pickle

# dicts_out = pp.parse_medline_xml('data/pubmed21n0001.xml.gz')
pmid2file = {}
data_dir = 'data'
#this guy change the dir of execution... 
os.chdir(data_dir)
#iterate on all xml.gz file
files = glob.glob("*.xml.gz")
pbar = tqdm(total=len(files))

for file in files:
    print("Processing :",file)
    # dicts_out = pp.parse_medline_xml(os.path.sep.join([data_dir,file]))
    dicts_out = pp.parse_medline_xml(file)

    for article in dicts_out:
        pmid2file[article['pmid']] = file
    


# print(pmid2file)

with open('pmid2file.pickle', 'wb') as handle:
    pickle.dump(pmid2file, handle, protocol=pickle.HIGHEST_PROTOCOL)

# with open('pmid2file.pickle', 'rb') as handle:
#     b = pickle.load(handle)
