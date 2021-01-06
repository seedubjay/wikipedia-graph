import pickle
from tqdm import tqdm

from db import DUMP_LANG, DUMP_DATE
RESULTS_DIR = 'results'

with open(f"{RESULTS_DIR}/{DUMP_LANG}wiki-{DUMP_DATE}-links-adjlist.csv") as f:
    
    edges = {}

    for l in tqdm(f):
        l = list(map(int, l.split(' ')))
        s = l[0]
        l = l[1:]
        edges[s] = l

    pickle.dump(edges, open(f"{RESULTS_DIR}/{DUMP_LANG}wiki-{DUMP_DATE}-adjlist.pkl", "wb"))
    print("Loaded", len(edges), "nodes")