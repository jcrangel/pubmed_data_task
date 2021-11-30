import pubmed_parser as pp


dicts_out = pp.parse_medline_xml('data/pubmed21n0001.xml.gz')

print('Number of articles',len(dicts_out))
for i in range(30):
    print('PMID',dicts_out[i]['pmid'])
    print('title:',dicts_out[i]['title'])
    print('abstract',dicts_out[i]['abstract'])

    