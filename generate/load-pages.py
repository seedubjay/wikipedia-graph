import os
from os import path
import sys
import requests
import json
import bz2
import multiprocessing as mp
from multiprocessing import Pool, Process, Manager, Value, Semaphore
from functools import partial
import pickle
from xml.etree import ElementTree
import re
import html
import shutil
from tqdm import tqdm
import time
import resource
from pymongo import MongoClient

from wikidata_scanner import parse_data_file

DUMP_LANG = 'en'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200901'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

DOWNLOAD_URL = f"https://dumps.wikimedia.org/{DUMP_LANG}wiki/{DUMP_DATE}/"

DOWNLOAD_DIR = 'downloads/'
if not path.isdir(DOWNLOAD_DIR): 
    os.mkdir(DOWNLOAD_DIR)

RESULTS_DIR = 'results/'
if not path.isdir(RESULTS_DIR): 
    os.mkdir(RESULTS_DIR)

def go(ids, ifile, semaphore):
    resource.setrlimit(resource.RLIMIT_RSS, (1<<28-1,1<<28-1))
    db_client = MongoClient('localhost', 27017)
    db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
    page_db = db.pages
    file_db = db.uploaded_files

    first_line = 0
    count = 0
    upload_info = file_db.find_one({'file': ifile})
    if upload_info is None:
        db.uploaded_files.insert_one({'file':ifile,'line':0,'page_count':0})
    else:
        if 'line' not in upload_info:
            # print('Skipped', ifile)
            semaphore.release()
            return
        first_line = upload_info['line']
        count = upload_info['page_count']

    def getID(title):
        if title in ids: return ids[title]
        return None

    print('Processing', ifile, 'from line', first_line)

    for i in parse_data_file(DOWNLOAD_DIR + ifile,getID,first_line):
        count += 1
        if count % 100 == 0 and 'line_count' in i:
            file_db.update_one({'file': ifile},{'$set' : {'line': i['line_count'], 'page_count': count}})
        page_db.update_one({'_id': i['page']['_id']}, {'$set': i['page']})

    db.uploaded_files.update_one({'file':ifile},{'$set': {'page_count': count}, '$unset': {'line':0}})

    print('Finished', ifile, f"({count} pages)")#, {len(local_redirects)} redirects, {len(local_graph)} edge groups)")
    semaphore.release()

if __name__ == '__main__':
    if not path.isfile(DOWNLOAD_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-dumpstatus.json"):
        with open(DOWNLOAD_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-dumpstatus.json", 'wb') as f:
            r = requests.get(DOWNLOAD_URL + 'dumpstatus.json', stream=True)
            f.writelines(r.iter_content(1024))

    with open(DOWNLOAD_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-dumpstatus.json") as f:
        dumpstatus = json.load(f)

    files = dumpstatus['jobs']['articlesmultistreamdump']['files']
    data_pages = sorted(list(filter(lambda f : 'index' not in f,files.keys())))

    for ifile in data_pages:
        if not path.isfile(DOWNLOAD_DIR + ifile) or not path.getsize(DOWNLOAD_DIR + ifile) == files[ifile]['size']:
            with open(DOWNLOAD_DIR + ifile, 'wb') as f:
                print('Downloading', ifile)
                r = requests.get(DOWNLOAD_URL + ifile, stream=True)
                f.writelines(r.iter_content(1024))

    data_pages.sort(key= lambda f : files[f]['size'], reverse=True)

    ids = {}
    if path.isfile(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-index.pkl"):
        with open(RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-index.pkl", 'rb') as f:
            print('load index from file')
            ids = pickle.load(f)

    else:
        db_client = MongoClient('localhost', 27017)
        db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
        page_db = db.pages
        print('load index from mongodb')
        for i in tqdm(page_db.find({},{'title':1}), total=page_db.estimated_document_count()):
            ids[i['title']] = i['_id']

    mp.set_start_method('fork')
    semaphore = Semaphore(6)
    processes = []
    for i in data_pages:
        semaphore.acquire()
        p = Process(target=go, args=(ids,i,semaphore))
        p.daemon = True
        processes.append(p)
        p.start()
    
    for p in processes: p.join()


