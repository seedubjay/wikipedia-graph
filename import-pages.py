import os
from os import path
import sys
import requests
import json
import bz2
from multiprocessing import Pool, Process, Manager, Value
from functools import partial
import pickle
from xml.etree import ElementTree
import re
import html
import shutil
from tqdm import tqdm
import time

from pymongo import MongoClient

db_client = MongoClient('localhost', 27017)
db = db_client.wikipedia

DUMP_LANG = 'en'
DUMP_DATE = '20200401'

DOWNLOAD_URL = f"https://dumps.wikimedia.org/{DUMP_LANG}wiki/{DUMP_DATE}/"

DOWNLOAD_DIR = 'downloads/'
if not path.isdir(DOWNLOAD_DIR): 
    os.mkdir(DOWNLOAD_DIR)

RESULTS_DIR = 'results/'
if not path.isdir(RESULTS_DIR): 
    os.mkdir(RESULTS_DIR)

TEMP_DIR = 'temp/'
if not path.isdir(TEMP_DIR): 
    os.mkdir(TEMP_DIR)

def collect_data_file(ifile, size):
    first_line = 0
    count = 0
    upload_info = db.uploaded_files.find_one({'file': ifile})
    if upload_info is None:
        db.uploaded_files.insert_one({'file':ifile,'line':0,'page_count':0})
    else:
        if 'line' not in upload_info:
            print('Skipped', ifile)
            return
        first_line = upload_info['line']
        count = upload_info['page_count']

    if not path.isfile(DOWNLOAD_DIR + ifile) or not path.getsize(DOWNLOAD_DIR + ifile) == size:
        with open(DOWNLOAD_DIR + ifile, 'wb') as f:
            r = requests.get(DOWNLOAD_URL + ifile, stream=True)
            f.writelines(r.iter_content(1024))
        print('Processing', ifile, '(w/ download)')
    else:
        print('Processing', ifile, 'from line', first_line)

    is_text = False
    ignore_page = False
    redirect_page = False
    title = None
    links = None
    ignore_text = False

    pattern = re.compile(r"\[\[(.+?)\]\]")

    with bz2.BZ2File(DOWNLOAD_DIR + ifile) as f:
        for (line_count,l) in enumerate(f):
            if line_count < first_line: continue

            l = l.decode('utf8').strip()
            if l.startswith('<page'):
                if count % 100 == 0: 
                    db.uploaded_files.update_one({'file': ifile},{'$set' : {'line': line_count, 'page_count': count}})
                count += 1
                
                ignore_page = False
                redirect_page = False
                title = None
                page_id = None
                links = set()
                continue

            if not ignore_page and not redirect_page:
                if l.startswith('<text'):
                    is_text = True
                    ignore_text = False
                    continue
                if l.endswith('</text>'):
                    is_text = False
                    continue
                if l.startswith('<title'):
                    title = html.unescape(l[7:-8])
                    x = db.pages.find_one({'title': title}, {'_id':1})
                    if x is None: ignore_page = True
                    else: page_id = x['_id']
                    continue
                if l.startswith('<redirect'):
                    redirect_page = True
                    to = html.unescape(l[17:-4])
                    x = db.pages.find_one({'title': to}, {'_id':1})
                    if x is not None:
                        #local_redirects[page_ids[title]] = page_ids[to]
                        db.pages.update_one({'_id': page_id}, {'$set': {'redirect': x['_id']}})
                    continue
                if l.endswith('</page>'):
                    #local_graph[page_ids[title]] = list(links)
                    l = []
                    for i in links:
                        x = db.pages.find_one({'title': i}, {'_id':1})
                        if x is not None: l.append(x['_id'])
                    db.pages.update_one({'_id': page_id}, {'$set': {'links': l}})
                    continue
                if is_text:
                    l = html.unescape(l)
                    if not ignore_text:
                        l = re.sub(r"<ref>.*?</ref>","<<REF>>",l)
                        l = l.replace("_"," ")
                        m = pattern.findall(l)
                        m = list(map(lambda s : s.split('|')[0].split('#')[0],m))
                        links.update(m)

    # with open(TEMP_DIR + ifile + '-graph.pkl', 'wb') as f:
    #     pickle.dump(local_graph, f)
    
    # with open(TEMP_DIR + ifile + '-redirects.pkl', 'wb') as f:
    #     pickle.dump(local_redirects, f)

    db.uploaded_files.update_one({'file':ifile},{'$set': {'page_count': count}, '$unset': {'line':0}})

    print('Finished', ifile, f"({count} pages)")#, {len(local_redirects)} redirects, {len(local_graph)} edge groups)")

if __name__ == '__main__':
    if not path.isfile(DOWNLOAD_DIR + 'dumpstatus.json'):
        with open(DOWNLOAD_DIR + 'dumpstatus.json', 'wb') as f:
            r = requests.get(DOWNLOAD_URL + 'dumpstatus.json', stream=True)
            f.writelines(r.iter_content(1024))

    with open(DOWNLOAD_DIR + 'dumpstatus.json') as f:
        dumpstatus = json.load(f)

    files = dumpstatus['jobs']['articlesmultistreamdump']['files']
    data_pages = sorted(list(filter(lambda f : 'index' not in f,files.keys())))

    # with open(RESULTS_DIR + 'index.pkl', 'rb') as f:
    #     page_ids = pickle.load(f)

    with Pool(16) as pool:
        pool.starmap(collect_data_file,[(x,files[x]['size']) for x in data_pages])
            