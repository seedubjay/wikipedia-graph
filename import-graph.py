from pymongo import MongoClient
from tqdm import tqdm

db_client = MongoClient('localhost', 27017)
page_db = db_client.wikipedia.pages
graph_db = db_client.wikipedia.graph

redirects = {}
redirect_count = list(page_db.aggregate([{'$match' : {'redirect' : {'$exists' : 1}}}, {'$count': 'count'}]))[0]['count']
for i in tqdm(page_db.find({'redirect' : {'$exists' : 1}}), total=redirect_count):
    redirects[i['_id']] = i['redirect']

x = redirects.keys()
for i in tqdm(x):
    j = i
    while j in redirects: j = redirects[j]
    if page_db.find_one({'_id': j, 'links': {'$exists': 1}}) is None: del redirects[i]

def follow_redirect(i):
    x = page_db.find_one({'_id': i, '$or': [{'redirect': {'$exists':1}},{'links': {'$exists':1}}]}, {'links': 0})
    if x is None: return None
    if 'redirect' in x: return follow_redirect(x['redirect'])
    return i

batch = []

for i in tqdm(page_db.find({'links' : {'$exists' : 1}}, {'links':1})):
    n = {'_id': i._id, 'links' : []}
    for l in i.links:
        x = follow_redirect(l)
        if x is not None: n['links'].append(x)
    batch.append(n)
    if len(batch) == 1000:
        graph_db.insert_many(batch)
        batch = []