import pickle5 as pickle

handle = open('pmid2entities_small.pickle','rb')
pmid2ent = pickle.load(handle)

print('loadaded')
input()