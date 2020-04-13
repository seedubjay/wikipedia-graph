from pymongo import MongoClient
from tqdm import tqdm
from os import path
import os

DUMP_LANG = 'en'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200401'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

db_client = MongoClient('localhost', 27017)
db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
page_db = db.pages
graph_db = db.graph

RESULTS_DIR = 'results/'
if not path.isdir(RESULTS_DIR): 
    os.mkdir(RESULTS_DIR)

pages = {}
redirects = {}

namespace = {
    0:'Article',
    1:'Talk',
    2:'User',
    3:'User talk',
    4:'Wikipedia',
    5:'Wikipedia talk',
    6:'File',
    7:'File talk',
    8:'MediaWiki',
    9:'MediaWiki talk',
    10:'Template',
    11:'Template talk',
    12:'Help',
    13:'Help talk',
    14:'Category',
    15:'Category talk',
    100:'Portal',
    101:'Portal talk',
    108:'Book',
    109:'Book talk',
    118:'Draft',
    119:'Draft talk',
    710:'TimedText',
    711:'TimedText talk',
    828:'Module',
    829:'Module talk',
}

with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-pages.csv", 'w') as f:
    f.write('page_id:ID,title,url,:LABEL')
    for i in tqdm(page_db.find(),total=page_db.estimated_document_count()):
        if 'links' in i:
            f.write(f"\n{i['_id']},\"{i['title']}\",\"{DUMP_LANG}.wikipedia.org/?curid={i['_id']}\",{namespace[i['namespace']]}")
            pages[i['_id']] = i
            
        elif 'redirect' in i: redirects[i['_id']] = i['redirect']

for i in tqdm(redirects):
    j = i
    while j in redirects:
        if j == redirects[j]: break
        j = redirects[j]
    redirects[i] = j

batch = []

with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-links.csv", 'w') as f:
    f.write(':START_ID,:END_ID,:TYPE')
    graph_db.drop()
    for i in tqdm(pages):
        n = {'_id': i['_id'], 'links' : [], 'namespace': i['namespace']}
        for l in i['links']:
            if l in redirects: l = redirects[l]
            if l in pages:
                n['links'].append(l)
                f.write(f"\n{i['_id']},{l},LINKS_TO")
        graph_db.insert_one(n)