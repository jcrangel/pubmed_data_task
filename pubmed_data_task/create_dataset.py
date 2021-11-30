import pandas as pd 
import pickle
import pubmed_parser as pp
import os
import json
import numpy as np
import random 


def get_data_pmid(pmid):
    """
    """
    global pubmed_file
    global dicts_articles
    #get file xml file that contains the pmid 
    try:
        pubmed_file_new = str(pmid2file[str(pmid)])
    except:
        return None
    
    # print('Found pmid in file:', pubmed_file_new)
    #If the pmid is in other file open it
    if pubmed_file_new != pubmed_file:
        pubmed_file = pubmed_file_new
        print('Opening:', pubmed_file)
        dicts_articles = pp.parse_medline_xml(os.path.sep.join([DATA_DIR,pubmed_file]))
    
    article_dict = find_article(dicts_articles,pmid)

    return article_dict if not None else None

def find_article(dicts_articles, pmid):
    """
    find the article with pmid in the list of articles
    """

    for article in dicts_articles:
        if article['pmid'] == str(pmid): 
            return article

    return None
# with open('data/pmid2file.pickle', 'rb') as handle:
#      = pickle.load(handle)

def process_context(text,start,end,num_words=16):

    #The start and end are not quite exact, we try to match the word
    #around start and end
    #hloride binding and the<target>Bohr</target> effect of human fetal erythrocytes and HbFII solutions.
    #Heruistic finding the correct start and end
    while start > 0:
        if text[start] in " ,.;())[]-":
            break
        start -= 1

    while end < len(text) :
        if text[end] in " ,.;()[]-":
            break
        end += 1

    left_words = text[0:start].split()#['the','C02',..]
    num_left_words = int(num_words/2  )  
    if len(left_words) < num_left_words:
        num_left_words = len(left_words)

    left_words = ' '.join(left_words[-num_left_words:])

    right_words = text[end:-1].split()  # ['the','C02',..]
    num_right_words = int(num_words/2)
    if len(right_words) < num_right_words:
        num_right_words = len(right_words)

    right_words = ' '.join(right_words[:num_right_words])
    left_words = left_words.replace('"', '')
    right_words = right_words.replace('"', '')

    # return left_words + ' <target> ' + str(mention) + ' </target> ' + right_words
    return left_words , right_words
    # return  '<target>' + text[start:end+1] + '</target>'

def create_dataset():
    global cui2thing
    n = 0
    idx = 0 
    ide2line = open(os.path.sep.join([DATASET_DIR,"id2line.json"]), "w", encoding='utf8')  # append mode
    ide2line.write("{\n")
    #File is huge work on chunkers 
    for df in pd.read_csv('data/Bio_entities_Main.csv', iterator=True, chunksize=1000):    
        # print(pd.DataFrame(df.groupby(["PMID"]).size().reset_index(name="Count")))
        #Group list of pmids in the chunk
        list_pmids_chuck = df.PMID.unique()
        for pmid in list_pmids_chuck:
            # print("Processing PMID:",pmid)
            #get abstract and title of the pmid
            article_dict = get_data_pmid(pmid)
            if article_dict == None:
                print("Pmid not found in files")
                continue

            # print('title:', article_dict['title'])
            # print('abstract:', article_dict['abstract'])
            article_text = article_dict['title'] + article_dict['abstract']
            #Process rows
            rows = df[df.PMID == pmid]
            for index,row in rows.iterrows():    
                # print(row)
                Mention = row.Mention
                EntityID = row.EntityID 
                Type = row.Type
                left_words = ''
                right_words = '' 
                try:
                    left_words,right_words = process_context(article_text, int(row.Start),int(row.End))
                    context = left_words + ' <target> ' + \
                        str(Mention) + ' </target> ' + right_words
                except:
                    continue

                if EntityID not in cui2thing:
                    # index,mention(cano),definition
                    defin = left_words + " " + str(Mention) + " "  + right_words
                    cui2thing[EntityID] = [str(idx), str(Mention), str(defin)]
                    idx += 1

                line = str(EntityID) + '\\t' + str(Type) + '\\t' + str(Mention) + '\\t' + str(context)
                n += 1 
                # ###DEBUG
                # if n == 100:
                # ###DEBUG
                ide2line.write('"{0}": "{1}",\n'.format(n,line))
                ide2line.flush()
 ###
    cui2emb = np.zeros((idx, 100))
    with open(os.path.sep.join([DATASET_DIR, 'cui2emb.pkl']), 'wb') as handle:
        pickle.dump(cui2emb, handle,
                    protocol=pickle.HIGHEST_PROTOCOL)
    ide2line.seek(ide2line.tell() - 2, os.SEEK_SET)
    ide2line.write('')
    ide2line.write("}\n")
    ide2line.close()

    #Creating the test train dev partitions of the dataset
    indexs = [i for i in range(n)]
    random.shuffle(indexs)
    lim_train = int(n * 0.70)
    lim_dev = lim_train + int(n*0.20)

    with open(os.path.sep.join([DATASET_DIR, 'train_mentionid.pkl']), 'wb') as handle:
        train_idx = indexs[:lim_train]
        pickle.dump(train_idx, handle,
                    protocol=pickle.HIGHEST_PROTOCOL)

    with open(os.path.sep.join([DATASET_DIR, 'dev_mentionid.pkl']), 'wb') as handle:
        dev_idx = indexs[lim_train:lim_dev]
        pickle.dump(dev_idx, handle,
                    protocol=pickle.HIGHEST_PROTOCOL)

    with open(os.path.sep.join([DATASET_DIR, 'test_mentionid.pkl']), 'wb') as handle:
        test_idx = indexs[lim_dev:-1]
        pickle.dump(test_idx, handle,
                    protocol=pickle.HIGHEST_PROTOCOL)

    return

## 
# 
#

DATA_DIR = 'data'
DATASET_DIR = 'dataset'
if not os.path.exists(DATASET_DIR):
    os.mkdir(DATASET_DIR)
print("Loading pmid to pubmed dict:")
handle = open('data/pmid2file.pickle', 'rb')
pmid2file = pickle.load(handle)
#we start with the
pubmed_file = 'pubmed21n0001.xml.gz'
print('Opening:', pubmed_file)
dicts_articles = pp.parse_medline_xml(
    os.path.sep.join([DATA_DIR, pubmed_file]))
#
cui2thing = {}

create_dataset()

print("Creating cui2cano cui2def cui2idx idx2cui...")
cui2cano_file = open(os.path.sep.join([DATASET_DIR, "cui2cano.json"]), "w", encoding='utf8')  # append mode
cui2def_file = open(os.path.sep.join([DATASET_DIR, "cui2def.json"]), "w", encoding='utf8')  # append mode
cui2idx_file = open(os.path.sep.join([DATASET_DIR, "cui2idx.json"]), "w", encoding='utf8')  # append mode
idx2cui_file = open(os.path.sep.join([DATASET_DIR, "idx2cui.json"]), "w", encoding='utf8')  # append mode

cui2idx_file.write("{\n")
idx2cui_file.write("{\n")
cui2cano_file.write("{\n")
cui2def_file.write("{\n")
# pprint.pprint(cui2thing)
for cui, value in cui2thing.items():
    cui2idx_file.write('"{0}": {1},\n'.format(cui, value[0]))
    idx2cui_file.write('"{0}": "{1}",\n'.format(value[0],cui))
    cui2cano_file.write('"{0}": "{1}",\n'.format(cui, value[1]))
    cui2def_file.write('"{0}": "{1}",\n'.format(cui, value[2]))
print("Finished sucessfully")

#Delet last comman an add the final } , -2 because the \n
cui2idx_file.seek(cui2idx_file.tell() - 2, os.SEEK_SET)
cui2idx_file.write('')
idx2cui_file.seek(idx2cui_file.tell() - 2, os.SEEK_SET)
idx2cui_file.write('')

cui2cano_file.seek(cui2cano_file.tell() - 2, os.SEEK_SET)
cui2cano_file.write('')

cui2def_file.seek(cui2def_file.tell() - 2, os.SEEK_SET)
cui2def_file.write('')

cui2idx_file.write("\n}")
idx2cui_file.write("\n}")
cui2cano_file.write("\n}")
cui2def_file.write("\n}")

# print(cui2thing)


