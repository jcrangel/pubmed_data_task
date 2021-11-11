#%%
import pubmed_parser as pp
import config
#%%
dicts_out = pp.parse_medline_xml(config.PUBMED_RAW_PATH+'/pubmed21n0001.xml.gz')

#%%
print('Number of articles',len(dicts_out))
for i in range(1):
    print('PMID',dicts_out[i]['pmid'])
    print('title:',dicts_out[i]['title'])
    print('abstract',dicts_out[i]['abstract'])
    print('abstract',dicts_out[i]['journal'])
    print('abstract',dicts_out[i]['pubdate'])

    
# %%
