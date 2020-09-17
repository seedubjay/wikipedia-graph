from pymongo import MongoClient
from tqdm import tqdm
from os import path
import os
import json

DUMP_LANG = 'en'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200401'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

db_client = MongoClient('localhost', 27017)
db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
page_db = db.pages
graph_db = db.graph

page_list = '''
Australia
United Kingdom
Great Barrier Reef
Coral reef
UNESCO
Algal bloom
Sugar
United States
Flag of Australia
New Zealand
Sydney
Radiocarbon dating
Joan of Arc
'''.strip().split('\n')

titles = {}
nodes = []
edges = []

for t in page_list:
    r = page_db.find_one({'title': t})
    titles[r['_id']] = r['title']

for i in titles.keys():
    nodes.append({"id": i, "label": titles[i]})
    for j in graph_db.find_one({'_id': i})["links"]:
        if j in titles and i != j: edges.append({"source": i, "target": j})

with open('analysis/subgraph.json', 'w') as f: 
    json.dump({"nodes": nodes, "edges": edges}, f)