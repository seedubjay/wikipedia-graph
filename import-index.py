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

DUMP_LANG = 'en'
DUMP_DATE = '20200401'

DOWNLOAD_URL = f"https://dumps.wikimedia.org/{DUMP_LANG}wiki/{DUMP_DATE}/"

start_blacklist = [
    ':',
    'image:',
    'wikipedia:',
    'wp:',
    'file:',
    'user:',
    'wikt:',
    'wikisource:',
    'talk:',
    'wiktionary:',
    's:',
    'voy:',
]

INDEX_DIR = 'downloads/'
if not path.isdir(INDEX_DIR): 
    os.mkdir(INDEX_DIR)

RESULTS_DIR = 'results/'
if not path.isdir(RESULTS_DIR): 
    os.mkdir(RESULTS_DIR)

def collect_index_file(ids, ifile, size):
    compressed_data = None
    if path.isfile(INDEX_DIR + ifile):
        with open(INDEX_DIR + ifile, 'rb') as f:
            compressed_data = f.read()
        if len(compressed_data) != size:
            print('Found corrupted', ifile)
            compressed_data = None

    if compressed_data is None:
        compressed_data = requests.get(DOWNLOAD_URL + ifile).content
        print('Downloaded', ifile)
        with open(INDEX_DIR + ifile, 'wb') as f:
            f.write(compressed_data)
    data = bz2.decompress(compressed_data).decode('utf8')
    
    d = {}
    for r in data.strip().split('\n'):
        c = r.split(':',2)
        d[html.unescape(c[2]).lower()] = int(c[1])

    ids.update(d)
    print('Added', ifile)

if __name__ == '__main__':
    dumpstatus = requests.get(DOWNLOAD_URL + 'dumpstatus.json').json()
    files = dumpstatus['jobs']['articlesmultistreamdump']['files']
    index_files = sorted(list(filter(lambda f : 'index' in f,files.keys())))

    print(len(index_files), 'files')

    manager = Manager()
    id_map = manager.dict()

    with Pool(8) as pool:
        pool.starmap(partial(collect_index_file,id_map),[(i,files[i]['size']) for i in index_files])

    with open(RESULTS_DIR + 'index.pkl', 'wb') as f:
        d = id_map.copy()
        print('Pickling', len(d), 'items')
        pickle.dump(d, f)