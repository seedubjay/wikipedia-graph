from pymongo import MongoClient
from tqdm import tqdm
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

categories = '''
People
History
Geography
Arts
Philosophy and religion
Everyday life
Society and social sciences
Biology and health sciences
Physical sciences
Technology
Mathematics
'''.strip().split('\n')

vital_pages = set()

for c in categories:
    page = page_db.find_one({'title': 'Wikipedia:Vital articles/Level/4/' + c})
    links = set(filter(lambda i : graph_db.find_one({'_id': i}) is not None, page['links']))
    print(c, len(links))
    vital_pages.update(links)

print(len(vital_pages), "pages")

with open('results/vital_pages_level4.json', 'w') as f: 
    json.dump(list(map(lambda i : page_db.find_one({'_id': i})['title'], vital_pages)), f)