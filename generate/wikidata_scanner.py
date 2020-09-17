# helper function to parse all links from wikipedia page

import bz2
import html
import re
from tqdm import tqdm
from pymongo import MongoClient
import os
import time
import sys
import pickle

loop_time = 0
decode_time = 0
text_time = 0
id_time = 0
probe_time = 0

# take first item in {i|...} and pass ... to functional to get list of links
template_links_with_args = {
    'main': lambda s:s.split('|'),
    'main list': lambda s:s.split('|'),
    'further': lambda s:s.split('|'),
    'See also': lambda s:s.split('|'),
    'details': lambda s:s.split('|'),
    'Redirect': lambda s: s.split('|')[2::2] if '|' in s else [s+' (disambiguation)'],
}

template_links = {}
with open('resources/country-codes.pkl', 'rb') as f:
    d = pickle.load(f)
    for i in d:
        template_links[i] = [d[i]]

def parse_data_file(ifile, getID, first_line = 0, verbose=True, timed=False):
    
    if timed:
        global loop_time
        global decode_time
        global text_time
        global id_time
        global probe_time

    pattern = re.compile(r"\[\[((?:(?!\[\[).)+?)\]\]")
    template_pattern = re.compile(r"{{((?:(?!{{).)+?)}}")

    with bz2.BZ2File(ifile) as f:
        is_text = False
        ignore_page = False
        redirect_page = False
        links = None
        namespace = None
        ignore_text = False
        page = None
        page_id = None

        if timed: loop_time -= time.time()

        for (line_count,l) in enumerate(f):
            if line_count < first_line:
                continue

            if timed: decode_time -= time.time()
            l = l.decode('utf8').strip()
            if timed: decode_time += time.time()
            
            if l.startswith('<page'):
                if timed: loop_time += time.time()
                if page is not None: yield {'page':page, 'line_count': line_count}
                if timed: loop_time -= time.time()

                if timed: probe_time -= time.time()
                page = None
                ignore_page = False
                redirect_page = False
                page_id = None
                links = set()
                namespace = None
                if timed: probe_time += time.time()
                continue

            if not ignore_page and not redirect_page:
                # if timed: probe_time -= time.time()
                # if l.startswith('<title'):
                #     for i in l: print(ord(i))

                #     title = html.unescape(l[7:-8])
                #     for i in title: print(ord(i))
                #     title = re.sub('\x85','\\n',title)
                #     for i in title: print(ord(i))
                #     page_id = getID(title)
                #     #assert page_id is not None, f"{bytes(l[7:-8],'utf8')}/{title} in {ifile}:{line_count}"
                #     continue
                if l.startswith('<id') and page_id is None:
                    page_id = int(l[4:-5])
                    continue
                if l.startswith('<ns>'):
                    namespace = int(l[4:-5])
                    if timed: probe_time += time.time()
                    continue
                if l.startswith('<redirect'):
                    redirect_page = True
                    to = html.unescape(l[17:-4])
                    if timed: id_time -= time.time()
                    if x := getID(to):
                        assert page is None
                        page = {'_id': page_id, 'redirect': x}
                    if timed: id_time += time.time()
                    if timed: probe_time += time.time()
                    continue
                if l.endswith('</page>'):
                    l = []
                    if timed: id_time -= time.time()
                    for i in links:
                        if '#' in i: i = i.split('#',1)[0]
                        if x := getID(i): l.append(x)
                        elif len(i) > 0:
                            if x := getID(i[0].upper() + i[1:]): l.append(x)
                    if timed: id_time += time.time()
                    assert page is None
                    assert page_id is not None
                    assert namespace is not None
                    page = {'_id': page_id, 'namespace': namespace, 'links': l}
                    if timed: probe_time += time.time()
                    continue
                if timed: text_time -= time.time()
                if l.startswith('<text') and not l.endswith('/>'):
                        is_text = True
                        l = l.split('>',1)[1]
                if is_text:
                    if not ignore_text:
                        l = html.unescape(l)
                        l = re.sub(r"<ref>.*?</ref>","<<REF>>",l)
                        l = l.replace("_"," ")
                        m = pattern.findall(l)
                        m = list(map(lambda s : s.split('|')[0],m))
                        links.update(m)
                        t = template_pattern.findall(l)
                        for i in t:
                            if '|' in i:
                                a,b = i.split('|',1)
                                if a in template_links_with_args:
                                    #print(page_id, a, template_links_with_args[a](b))
                                    links.update(template_links_with_args[a](b))
                            elif i in template_links:
                                #print(page_id, i, template_links[i])
                                links.update(template_links[i])
                                
                if l.endswith('</text>'):
                    is_text = False
                if timed: text_time += time.time()
                if timed: probe_time += time.time()
        loop_time += time.time()
        if page is not None:
            yield {'page': page}

if __name__ == '__main__':
    ifile = 'downloads/enwiki-20200901-pages-articles-multistream2.xml-p30304p88444.bz2'

    DUMP_LANG = 'en'
    if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
    DUMP_DATE = '20200901'
    if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']

    db_client = MongoClient('localhost', 27017)
    db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
    page_db = db.pages
    file_db = db.uploaded_files

    # ids = {}
    # for i in tqdm(page_db.find({},{'title':1}),total=page_db.estimated_document_count()):
    #     ids[i['title']] = i['_id']
    
    # print(sys.getsizeof(ids))

    # def getID(title):
    #     if title in ids: return ids[title]
    #     return None

    def getID(title):
        x = page_db.find_one({'title': title}, {'_id':1})
        if x is None: return None
        return x['_id']

    start_time = time.time()

    for i in parse_data_file(ifile, getID, 4994196):
        print(i)
        break

    # times = {
    #     'decode': lambda:decode_time,
    #     'text': lambda:text_time,
    #     'loop': lambda:loop_time,
    #     'id': lambda:id_time,
    #     'probe': lambda:probe_time,
    # }

    # print('\n'*len(times))
    # for (count,i) in enumerate(parse_data_file(ifile, getID,verbose=False,timed=True)):
    #     total_time = time.time() - start_time
    #     print('\033[F'*(len(times)+1),end='')
    #     for t in times:
    #         print(f"{t}: {round(times[t]()/total_time*10000)/100}%")
    #     print(f"{round(count/total_time)} it/s")
