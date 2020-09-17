import os
from os import path
import sys
import requests
import json
import bz2
from multiprocessing import Pool, Manager
from functools import partial
import pickle
import html
from tqdm import tqdm
import shutil
from pymongo import MongoClient

DUMP_LANG = 'en'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200901'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

db_client = MongoClient('localhost', 27017)
db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
page_db = db.pages
file_db = db.uploaded_files

DOWNLOAD_URL = f"https://dumps.wikimedia.org/{DUMP_LANG}wiki/{DUMP_DATE}/"

DOWNLOAD_DIR = 'downloads/'
if not path.isdir(DOWNLOAD_DIR): 
    os.mkdir(DOWNLOAD_DIR)

RESULTS_DIR = 'results/'
if not path.isdir(RESULTS_DIR): 
    os.mkdir(RESULTS_DIR)

def collect_index_file(ifile, size):
    d = []
    # local_start_count = {}

    with bz2.BZ2File(DOWNLOAD_DIR + ifile) as f:
        for r in f:
            r = r.decode('utf8').strip()
            c = r.split(':',2)
            i = int(c[1])
            title = html.unescape(c[2])
            # for b in start_blacklist:
                # if title.startswith(b):
                #     if b not in local_start_count: local_start_count[b] = start_count[b]
                #     local_start_count[b] += 1

            d.append({'title':title, '_id':i})

    page_db.insert_many(d)
    print('Added', ifile)

if __name__ == '__main__':
    if not path.isfile(DOWNLOAD_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-dumpstatus.json"):
        with open(DOWNLOAD_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-dumpstatus.json", 'wb') as f:
            r = requests.get(DOWNLOAD_URL + 'dumpstatus.json', stream=True)
            f.writelines(r.iter_content(1024))

    with open(DOWNLOAD_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-dumpstatus.json") as f:
        dumpstatus = json.load(f)
    
    files = dumpstatus['jobs']['articlesmultistreamdump']['files']
    index_files = sorted(list(filter(lambda f : 'index' in f,files.keys())))

    print(len(index_files), 'files')

    for ifile in index_files:
        if not path.isfile(DOWNLOAD_DIR + ifile) or not path.getsize(DOWNLOAD_DIR + ifile) == files[ifile]['size']:
            with open(DOWNLOAD_DIR + ifile, 'wb') as f:
                r = requests.get(DOWNLOAD_URL + ifile, stream=True)
                f.writelines(r.iter_content(1024))
                print('Downloaded', ifile)

    index_filename = RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-index.pkl"

    page_db.drop()
    if path.isfile(index_filename): os.remove(index_filename)
    file_db.drop()
    page_db.create_index([('title',1)])

    try:
        with Pool(8) as pool:
            pool.starmap(collect_index_file,[(i,files[i]['size']) for i in index_files])
    except:
        print('Import failed')
        page_db.drop()
        raise

    print('Pickling IDs')
    ids = {}
    for i in tqdm(page_db.find({},{'title':1}),total=page_db.estimated_document_count()):
        ids[i['title']] = i['_id']
    
    with open(index_filename, 'wb') as f:
        pickle.dump(ids,f)