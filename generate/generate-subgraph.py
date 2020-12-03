from pymongo import MongoClient
from tqdm import tqdm
import sys
from os import path
import os
import json

DUMP_LANG = 'en'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200901'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

db_client = MongoClient('localhost', 27017)
db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
page_db = db.pages
graph_db = db.graph

with open(sys.argv[1]) as f:
    page_list = json.load(f)

titles = {}
nodes = []
edges = []

for t in tqdm(page_list):
    r = page_db.find_one({'title': t})
    if r["namespace"] == 0: titles[r['_id']] = r['title']

ids = list(titles.keys())

forward_adj = {i:[] for i in ids}
reverse_adj = {i:[] for i in ids}

for i in tqdm(ids):
    nodes.append({"id": i, "label": titles[i]})
    for j in graph_db.find_one({'_id': i})["links"]:
        if j in titles and i != j:
            edges.append({"source": i, "target": j})
            forward_adj[i].append(j)
            reverse_adj[j].append(i)

def find_disconnected(nodes, adj):
    unseen = set(nodes)
    unseen.remove(nodes[0])
    active = nodes[:1]
    while len(active) > 0:
        i = active.pop()
        for j in adj[i]:
            if j in unseen: 
                unseen.remove(j)
                active.append(j)
    return unseen

unreachable_ends = find_disconnected(ids, forward_adj)
unreachable_starts = find_disconnected(ids, reverse_adj)

if len(unreachable_ends) > 0: print(len(unreachable_ends), "unreachable ends: ", list(unreachable_ends))
if len(unreachable_starts) > 0: print(len(unreachable_starts), "unreachable ends: ", list(unreachable_starts))

if len(sys.argv) > 2:
    with open(sys.argv[2], 'w') as f: 
        json.dump({"nodes": nodes, "edges": edges}, f, indent=2)