


import pickle
from pubmed_data_task.dataset_creator import create_pmid2details_ontologies,load_EL_onto
from pubmed_data_task import config

e_linker = load_EL_onto()

pmid2details = create_pmid2details_ontologies(config.PUBMED_RAW_PATH,e_linker)
print('saving file pmid2details.pickle')
handle = open('pmid2details_onto.pickle', 'wb')
pickle.dump(pmid2details, handle, protocol=pickle.HIGHEST_PROTOCOL)
