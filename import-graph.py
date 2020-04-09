import os
from os import path
import sys
import requests
import json
import bz2
from multiprocessing import Pool, Manager
from functools import partial
import pickle
from xml.etree import ElementTree
import re
import html

DUMP_LANG = 'en'
DUMP_DATE = '20200401'

DOWNLOAD_URL = f"https://dumps.wikimedia.org/{DUMP_LANG}wiki/{DUMP_DATE}/"

DOWNLOAD_DIR = 'downloads/'
if not path.isdir(DOWNLOAD_DIR): 
    os.mkdir(DOWNLOAD_DIR)

RESULTS_DIR = 'results/'
if not path.isdir(RESULTS_DIR): 
    os.mkdir(RESULTS_DIR)

def collect_data_file(page_ids, graph, redirects, ifile, size):
    if not path.isfile(DOWNLOAD_DIR + ifile) or not path.getsize(DOWNLOAD_DIR + ifile) == size:
        with open(DOWNLOAD_DIR + ifile, 'wb') as f:
            r = requests.get(DOWNLOAD_URL + ifile, stream=True)
            f.writelines(r.iter_content(1024))
        print('Processing', ifile, '(w/ download)')
    else:
        print('Processing', ifile)

    is_text = False
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
            if not redirect_page:
                if l.startswith('<text'):
                    is_text = True
                    ignore_text = False
                    continue
                if l.endswith('</text>'):
                    is_text = False
                if l.startswith('<title'):
                    title = html.unescape(l[7:-8]).lower()
                if l.startswith('<redirect'):
                    redirect_page = True
                    to = html.unescape(l[17:-4]).lower()
                    if to in page_ids:
                        local_redirects[page_ids[title]] = page_ids[to]
                if is_text:
                    l = html.unescape(l.lower())
                    if not ignore_text:
                        l = re.sub(r"<ref>.*?</ref>","<<REF>>",l)
                        l = l.replace("_"," ")
                        m = pattern.findall(l)
                        m = list(map(lambda s : s.split('|')[0].split('#')[0],m))
                        links.update(filter(lambda s : s in page_ids,m))
                if l.endswith('</page>'):
                    local_graph[page_ids[title]] = list(links)
                
            if l.startswith('<page'):
                count += 1
                redirect_page = False
                title = None
                links = set()
    

    graph.update(local_graph)
    redirects.update(local_redirects)
    print('Finished', ifile, f"({count} pages)")
            

if __name__ == '__main__':
    dumpstatus = requests.get(DOWNLOAD_URL + 'dumpstatus.json').json()
    files = dumpstatus['jobs']['articlesmultistreamdump']['files']
    data_files = sorted(list(filter(lambda f : 'index' not in f,files.keys())))

    with open(RESULTS_DIR + 'index.pkl', 'rb') as f:
        pi = pickle.load(f)

    manager = Manager()
    page_ids = manager.dict()
    page_ids = pi
    print('Index loaded')

    graph = manager.dict()
    redirects = manager.dict()

    with Pool(4) as pool:
        pool.starmap(partial(collect_data_file, page_ids, graph, redirects),[(i,files[i]['size']) for i in data_files])

    with open(RESULTS_DIR + 'redirects.pkl', 'wb') as f:
        d = redirects.copy()
        print('Pickling', len(d), 'redirects')
        pickle.dump(d, f)
    
    with open(RESULTS_DIR + 'raw_links.pkl', 'wb') as f:
        d = graph.copy()
        print('Pickling', len(d), 'pages of links')
        pickle.dump(d, f)