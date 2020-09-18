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
    titles[r['_id']] = r['title']

for i in tqdm(titles.keys()):
    nodes.append({"id": i, "label": titles[i]})
    for j in graph_db.find_one({'_id': i})["links"]:
        if j in titles and i != j: edges.append({"source": i, "target": j})

with open(sys.argv[2], 'w') as f: 
    json.dump({"nodes": nodes, "edges": edges}, f, indent=2)