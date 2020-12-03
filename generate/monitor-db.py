from pymongo import MongoClient
import time
from tqdm import tqdm 
import os

from db import db

page_db = db.pages
file_db = db.uploaded_files

def page_count():
    return list(file_db.aggregate([{'$group': {'_id': None, 'total': {'$sum': '$page_count'}}}]))[0]['total']

count = page_count()

with tqdm(initial=count, total=page_db.estimated_document_count(), smoothing=0.1) as pbar:
    while True:
        time.sleep(.2)
        prev = count
        count = page_count()
        pbar.update(count-prev)
