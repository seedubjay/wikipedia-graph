import os
from os import path
import sys
import json
import html
import bz2
from tqdm import tqdm

SEARCH_TITLE = 'Great Barrier Reef'

DUMP_LANG = 'en'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200901'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

DOWNLOAD_DIR = 'downloads/'

with open(DOWNLOAD_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-dumpstatus.json") as f:
    dumpstatus = json.load(f)

files = dumpstatus['jobs']['articlesmultistreamdump']['files']
index_files = sorted(list(filter(lambda f : 'index' in f,files.keys())))

for ifile in tqdm(index_files):
    with bz2.BZ2File(DOWNLOAD_DIR + ifile) as f:
        for r in f:
            r = r.decode('utf8').strip()
            c = r.split(':',2)
            i = int(c[1])
            title = html.unescape(c[2])
            if len(title) > 0 and title[0] != title[0].upper(): print(title)
            if title == SEARCH_TITLE: print(i, SEARCH_TITLE, ifile)