import os
from pymongo import MongoClient

DUMP_LANG = 'en'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200401'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

db_client = MongoClient('localhost', 27017)
db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
page_db = db.pages
graph_db = db.graph

RESULTS_DIR = 'results/'

pages = {}
with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-pages.csv") as f:
    for l in f:
        