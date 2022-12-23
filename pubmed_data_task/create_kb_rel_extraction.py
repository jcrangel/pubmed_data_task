
import networkx
import obonet
import os
import pickle
url = '/home/julio/repos/dkouqe/data/ontologies/obo_files/chebi/chebi.obo'
graph = obonet.read_obo(url)  # Number of nodes
print(len(graph))
graph.number_of_edges()

lines = []
for id,data in graph.nodes(data=True):
    #exploring is_a relations
    if 'is_a' in data:
        for rel in data['is_a']:
            lines.append(('is_a',id,rel))
    if 'relationship' in data:
        for _rel in data['relationship']:
            rel, id2 = _rel.split(' ')
            lines.append((rel,id,id2))

with open(os.path.sep.join(['kb_rel_extraction.pickle']), 'wb') as handle:
    pickle.dump(lines,
                handle, protocol=pickle.HIGHEST_PROTOCOL)


