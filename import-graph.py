from pymongo import MongoClient
from tqdm import tqdm

DUMP_LANG = 'en'
DUMP_DATE = '20200401'

db_client = MongoClient('localhost', 27017)
page_db = db_client.wikipedia.pages
graph_db = db_client.wikipedia.graph

page_list = set()
redirects = {}

for i in tqdm(page_db.find(),total=page_db.estimated_document_count()):
    if 'links' in i: page_list.add(i['_id'])
    elif 'redirect' in i: redirects[i['_id']] = i['redirect']

for i in tqdm(redirects):
    j = i
    while j in redirects:
        if j == redirects[j]: break
        j = redirects[j]
    redirects[i] = j

batch = []

for i in tqdm(page_db.find({'links' : {'$exists' : 1}}, {'links':1}), total=len(page_list)):
    n = {'_id': i['_id'], 'links' : []}
    for l in i['links']:
        if l in redirects: l = redirects[l]
        if l in page_list: n['links'].append(l)
    batch.append(n)
    if len(batch) == 5000:
        graph_db.insert_many(batch)
        batch = []
graph_db.insert_many(batch)