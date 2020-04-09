import os
from os import path
import sys
import requests
import json
import bz2
from multiprocessing import Pool, Process, Manager
from functools import partial
import pickle
from xml.etree import ElementTree
import re
import html
import shutil

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
    if path.isfile(TEMP_DIR + ifile + '-graph.pkl') and path.isfile(TEMP_DIR + ifile + '-redirects.pkl'):
        print('Skipped', ifile)
        return
    if not path.isfile(DOWNLOAD_DIR + ifile) or not path.getsize(DOWNLOAD_DIR + ifile) == size:
        with open(DOWNLOAD_DIR + ifile, 'wb') as f:
            r = requests.get(DOWNLOAD_URL + ifile, stream=True)
            f.writelines(r.iter_content(1024))
        print('Processing', ifile, '(w/ download)')
    else:
        print('Processing', ifile)

    is_text = False
    ignore_page = False
    redirect_page = False
    title = None
    links = None
    ignore_text = False
    count = 0

    pattern = re.compile(r"\[\[(.+?)\]\]")

    local_redirects = {}
    local_graph = {}

    with bz2.BZ2File(DOWNLOAD_DIR + ifile) as f:
        for l in f:
            l = l.decode('utf8').strip()

            if l.startswith('<page'):
                count += 1
                ignore_page = False
                redirect_page = False
                title = None
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
                    if title not in page_ids: ignore_page = True
                    continue
                if l.startswith('<redirect'):
                    redirect_page = True
                    to = html.unescape(l[17:-4])
                    if to in page_ids:
                        local_redirects[page_ids[title]] = page_ids[to]
                    continue
                if l.endswith('</page>'):
                    local_graph[page_ids[title]] = list(links)
                    continue
                if is_text:
                    l = html.unescape(l)
                    if not ignore_text:
                        l = re.sub(r"<ref>.*?</ref>","<<REF>>",l)
                        l = l.replace("_"," ")
                        m = pattern.findall(l)
                        m = list(map(lambda s : s.split('|')[0].split('#')[0],m))
                        links.update(filter(lambda s : s in page_ids,m))

    
    with open(TEMP_DIR + ifile + '-graph.pkl', 'wb') as f:
        pickle.dump(local_graph, f)
    
    with open(TEMP_DIR + ifile + '-redirects.pkl', 'wb') as f:
        pickle.dump(local_redirects, f)

    print('Finished', ifile, f"({count} pages, {len(local_redirects)} redirects, {len(local_graph)} edge groups)")

if not path.isfile(DOWNLOAD_DIR + 'dumpstatus.json'):
    with open(DOWNLOAD_DIR + 'dumpstatus.json', 'wb') as f:
        r = requests.get(DOWNLOAD_URL + 'dumpstatus.json', stream=True)
        f.writelines(r.iter_content(1024))

with open(DOWNLOAD_DIR + 'dumpstatus.json') as f:
    dumpstatus = json.load(f)

files = dumpstatus['jobs']['articlesmultistreamdump']['files']
data_pages = sorted(list(filter(lambda f : 'index' not in f,files.keys())))

with open(RESULTS_DIR + 'index.pkl', 'rb') as f:
    page_ids = pickle.load(f)

for i in data_pages:
    collect_data_file(i,files[i]['size'])

# merge
graph = {}
for i in os.listdir(TEMP_DIR):
    if i.endswith('-graph.pkl'):
        with open(i,'rb') as f:
            d = pickle.load(f)
            graph.update(d)
with open(RESULTS_DIR + 'raw_graph.pkl','wb') as f:
    pickle.dump(graph,f)
del graph

redirects = {}
for i in os.listdir(TEMP_DIR):
    if i.endswith('-redirects.pkl'):
        with open(i,'rb') as f:
            d = pickle.load(f)
            redirects.update(d)
with open(RESULTS_DIR + 'redirects.pkl','wb') as f:
    pickle.dump(redirects,f)
del redirects

shutil.rmtree(TEMP_DIR)