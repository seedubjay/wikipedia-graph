import bz2
import html
import re
from tqdm import tqdm
from pymongo import MongoClient
import os
import time

loop_time = 0
decode_time = 0
text_time = 0
id_time = 0

def parse_data_file(ifile, getID, first_line = 0, verbose=True, timed=False):
    
    if timed:
        global loop_time
        global decode_time
        global text_time
        global id_time
    
    is_text = False
    ignore_page = False
    redirect_page = False
    title = None
    links = None
    namespace = None
    ignore_text = False
    page = None

    pattern = re.compile(r"\[\[((?:(?!\[\[).)+?)\]\]")

    with bz2.BZ2File(ifile) as f:
        for (line_count,l) in enumerate(f):
            if line_count < first_line:
                continue

            if timed: decode_time -= time.time()
            l = l.decode('utf8').strip()
            if timed: decode_time += time.time()

            if l.startswith('<page'):
                if page is not None: yield {'page':page, 'line_count': line_count}
                page = None
                ignore_page = False
                redirect_page = False
                title = None
                page_id = None
                links = None
                namespace = None
                continue

            if not ignore_page and not redirect_page:
                if timed: loop_time -= time.time()
                if l.startswith('<title'):
                    title = html.unescape(l[7:-8])
                    if timed: loop_time += time.time()
                    continue
                if l.startswith('<ns>'):
                    namespace = int(l[4:-5])
                    if timed: loop_time += time.time()
                    continue
                if l.startswith('<id>'):
                    page_id = int(l[4:-5])
                    if timed: loop_time += time.time()
                    continue
                if l.startswith('<redirect'):
                    redirect_page = True
                    to = html.unescape(l[17:-4])
                    if timed: id_time -= time.time()
                    x = getID(to)
                    if timed: id_time += time.time()
                    if timed: loop_time += time.time()
                    if x is not None:
                        assert page is None
                        page = {'_id': page_id, 'redirect': x}
                    continue
                if l.endswith('</page>'):
                    l = []
                    if timed: id_time -= time.time()
                    if links is None: print(ifile, line_count)
                    for i in links:
                        x = getID(i)
                        if x is not None: l.append(x)
                    if timed: id_time += time.time()
                    if timed: loop_time += time.time()
                    assert page is None
                    page = {'_id': page_id, 'namespace': namespace, 'links': l}
                    continue
                if timed: loop_time += time.time()
                if l.startswith('<text'):
                    links = set()
                    if not l.endswith('/>'):
                        is_text = True
                        l = l.split('>',1)[1]
                if is_text:
                    if timed: text_time -= time.time()
                    if not ignore_text:
                        l = html.unescape(l)
                        l = re.sub(r"<ref>.*?</ref>","<<REF>>",l)
                        l = l.replace("_"," ")
                        m = pattern.findall(l)
                        m = list(map(lambda s : s.split('|')[0].split('#')[0],m))
                        if links is None: print(ifile, line_count)
                        links.update(m)
                    if timed: text_time += time.time()
                if l.endswith('</text>'):
                    is_text = False
    if page is not None:
        yield {'page': page}

if __name__ == '__main__':
    ifile = 'downloads/ptwiki-20200401-pages-articles-multistream5.xml-p2522163p4022162.bz2'

    DUMP_LANG = 'en'
    if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
    DUMP_DATE = '20200401'
    if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

    db_client = MongoClient('localhost', 27017)
    db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
    page_db = db.pages
    file_db = db.uploaded_files

    # ids = {}
    # for i in tqdm(page_db.find({},{'title':1}),total=page_db.estimated_document_count()):
    #     ids[i['title']] = i['_id']

    def getID(title):
        # if title in ids: return ids[title]
        # return None
        x = page_db.find_one({'title': title}, {'_id':1})
        if x is None: return None
        return x['_id']

    start_time = time.time()

    for i in parse_data_file(ifile, getID, 183454): pass

    # print()
    # print()
    # print()
    # print()
    # print()
    # for i in parse_data_file(ifile, getID,verbose=False,timed=True):
    #     total_time = time.time() - start_time
    #     print('\033[F\033[F\033[F\033[F', end='')
    #     print(f"decode: {round(decode_time/total_time*10000)/100}%")
    #     print(f"text: {round(text_time/total_time*10000)/100}%")
    #     print(f"loop: {round(loop_time/total_time*10000)/100}%")
    #     print(f"getID: {round(id_time/total_time*10000)/100}%")
        

