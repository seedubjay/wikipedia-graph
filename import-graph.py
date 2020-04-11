from pymongo import MongoClient
from tqdm import tqdm
from os import path
import os

DUMP_LANG = 'pt'
DUMP_DATE = '20200401'

db_client = MongoClient('localhost', 27017)
db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
page_db = db.pages
graph_db = db.graph

RESULTS_DIR = 'results/'
if not path.isdir(RESULTS_DIR): 
    os.mkdir(RESULTS_DIR)

pages = {}
redirects = {}

start_blacklist = [
    'Talk:',
    'User:',
    'User talk:',
    'Wikipedia:',
    'Wikipedia talk:',
    'File:',
    'File talk:',
    'MediaWiki:',
    'MediaWiki talk:',
    'Template:',
    'Template talk:',
    'Help:',
    'Help talk:',
    'Category:',
    'Category talk:',
    'Portal:',
    'Portal talk:',
    'Draft:',
    'Draft talk:',
    'TimedText:',
    'TimedText talk:',
    'Module:',
    'Module talk:',
]

with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-pages.csv", 'w') as f:
    for i in tqdm(page_db.find(),total=page_db.estimated_document_count()):
        if 'links' in i:
            if len(pages) > 0: f.write('\n')
            label = 'Article'
            for b in start_blacklist:
                if i['title'].startswith(b): label = b[:-1]
            f.write(f"{i['_id']},\"{i['title']}\",{label}")
            pages[i['_id']] = i['title']
            
        elif 'redirect' in i: redirects[i['_id']] = i['redirect']

for i in tqdm(redirects):
    j = i
    while j in redirects:
        if j == redirects[j]: break
        j = redirects[j]
    redirects[i] = j

batch = []

with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-links.csv", 'w') as f:
    first_row = True
    graph_db.drop()
    for i in tqdm(page_db.find({'links' : {'$exists' : 1}}, {'links':1}), total=len(pages)):
        n = {'_id': i['_id'], 'links' : []}
        for l in i['links']:
            if l in redirects: l = redirects[l]
            if l in pages:
                n['links'].append(l)
                if not first_row: f.write('\n')
                first_row = False
                f.write(f"{i['_id']},{l}")
        batch.append(n)
        if len(batch) == 100:
            graph_db.insert_many(batch)
            batch = []
    graph_db.insert_many(batch)