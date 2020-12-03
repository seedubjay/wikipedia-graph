# takes page database and generates graph database, .gt file for jupyter and neo4j files

from tqdm import tqdm
from os import path
import os
import csv

from db import db, DUMP_LANG, DUMP_DATE

page_db = db.pages
graph_db = db.graph

RESULTS_DIR = 'results/'
if not path.isdir(RESULTS_DIR): 
    os.mkdir(RESULTS_DIR)

ids = set()
redirects = {}

namespace = {
    0:'Article',
    # 1:'Talk',
    # 2:'User',
    # 3:'User talk',
    # 4:'Wikipedia',
    # 5:'Wikipedia talk',
    # 6:'File',
    # 7:'File talk',
    # 8:'MediaWiki',
    # 9:'MediaWiki talk',
    # 10:'Template',
    # 11:'Template talk',
    # 12:'Help',
    # 13:'Help talk',
    # 14:'Category',
    # 15:'Category talk',
    # 100:'Portal',
    # 101:'Portal talk',
    # 108:'Book',
    # 109:'Book talk',
    # 118:'Draft',
    # 119:'Draft talk',
    # 710:'TimedText',
    # 711:'TimedText talk',
    # 828:'Module',
    # 829:'Module talk',
}


with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-pages.csv", 'w') as f:
    create = csv.writer(f,quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
    for i in tqdm(page_db.find(),total=page_db.estimated_document_count()):
        if 'links' in i:
            if i['namespace'] in namespace:
                create.writerow([i['_id'],i['title'],namespace[i['namespace']]])
                ids.add(i['_id'])
        elif 'redirect' in i: redirects[i['_id']] = i['redirect']

for i in tqdm(redirects):
    seen = set()
    j = i
    while j in redirects:
        if j in seen: break
        seen.add(j)
        j = redirects[j]
    redirects[i] = j

batch = []

update_mongodb = True

with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-links-neo4j.csv", 'w') as fneo4j:
    with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-links-adjlist.csv", 'w') as fadj:
        with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-links-edgelist.csv", 'w') as f:
            fneo4j.write(':START_ID,:END_ID,:TYPE')
            if update_mongodb: graph_db.drop()
            for i in tqdm(page_db.find({'links':{'$exists':1}}),total=page_db.estimated_document_count()-len(redirects)):
                if i['namespace'] not in namespace: continue
                n = {'_id': i['_id'], 'links' : [], 'namespace': i['namespace']}
                for l in i['links']:
                    if l in redirects: l = redirects[l]
                    if l in ids and l != i['_id']:
                        n['links'].append(l)
                        fneo4j.write(f"\n{i['_id']},{l},LINKS_TO")
                        f.write(f"{i['_id']} {l}\n")
                        
                if len(n['links']) > 0: fadj.write(' '.join(map(str,[n['_id']] + n['links'])) + '\n')
                if update_mongodb: batch.append(n)
                if len(batch) == 1000:
                    graph_db.insert_many(batch)
                    batch = []
            if len(batch) > 0: graph_db.insert_many(batch)
