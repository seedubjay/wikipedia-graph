import pickle
from tqdm import tqdm

from db import DUMP_LANG, DUMP_DATE
RESULTS_DIR = 'results'

N = 10

with open(f"{RESULTS_DIR}/{DUMP_LANG}wiki-{DUMP_DATE}-links-adjlist.csv") as f:
    
    nodes = set()
    edges = {}
    indegree = {}

    for l in tqdm(f):
        l = list(map(int, l.split(' ')))
        s = l[0]
        l = l[1:]
        for i in l:
            if i not in indegree:
                indegree[i] = 0
            indegree[i] += 1
        if len(l) < N: continue
        nodes.add(s)
        edges[s] = l

    nodes = set(filter(lambda x : x in indegree and indegree[x] >= N, nodes))
    
    edges2 = {}
    for n in tqdm(nodes):
        edges2[n] = list(filter(lambda x : x in nodes, edges[n]))
    
    pickle.dump(edges2, open(f"{RESULTS_DIR}/{DUMP_LANG}wiki-{DUMP_DATE}-degree-{N}-adjlist.pkl", "wb"))
    print("Loaded", len(nodes), "nodes")