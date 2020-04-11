from pymongo import MongoClient
import time
from tqdm import tqdm 

DUMP_LANG = 'sa'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200401'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

db_client = MongoClient('localhost', 27017)
db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
page_db = db.pages
file_db = db.uploaded_files

def page_count():
    return list(file_db.aggregate([{'$group': {'_id': None, 'total': {'$sum': '$page_count'}}}]))[0]['total']

count = page_count()

with tqdm(initial=count, total=page_db.estimated_document_count()) as pbar:
    while True:
        time.sleep(.2)
        prev = count
        count = page_count()
        pbar.update(count-prev)
