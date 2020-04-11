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

from pymongo import MongoClient

DUMP_LANG = 'sa'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200401'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

db_client = MongoClient('localhost', 27017)
db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
page_db = db.pages

DOWNLOAD_URL = f"https://dumps.wikimedia.org/{DUMP_LANG}wiki/{DUMP_DATE}/"

start_blacklist = [
    # 'Talk:',
    # 'User:',
    # 'User talk:',
    # 'Wikipedia:',
    # 'Wikipedia talk:',
    # 'File:',
    # 'File talk:',
    # 'MediaWiki:',
    # 'MediaWiki talk:',
    # 'Template:',
    # 'Template talk:',
    # 'Help:',
    # 'Help talk:',
    # 'Category talk:',
    # 'Portal:',
    # 'Portal talk:',
    # 'Draft:',
    # 'Draft talk:',
    # 'TimedText:',
    # 'TimedText talk:',
    # 'Module:',
    # 'Module talk:',
]

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

            if not any(map(lambda b : title.startswith(b), start_blacklist)):
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

    manager = Manager()
    id_map = manager.dict()
    start_count = manager.dict()
    for b in start_blacklist: start_count[b] = 0

    page_db.drop()
    page_db.create_index([('title',1)])

    try:
        with Pool(8) as pool:
            pool.starmap(collect_index_file,[(i,files[i]['size']) for i in index_files])
    finally:
        page_db.drop()
