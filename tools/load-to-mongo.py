import sys
from pymongo import MongoClient
import pickle
from tqdm import tqdm

db_client = MongoClient('localhost', 27017)
db = db_client["wikipedia"]

edges = pickle.load(open(sys.argv[1], "rb"))
batch = []
for s in tqdm(edges):
    batch.append({'_id': s, 'links': edges[s]})
    if len(batch) >= 1000:
        db.graph.insert_many(batch)
        batch = [] 
db.graph.insert_many(batch)
