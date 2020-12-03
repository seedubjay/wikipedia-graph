from tqdm import tqdm
import sys
import os
import numpy as np
import csv

sys.path.append('/usr/local/Cellar/graph-tool/2.31_1/lib/python3.8/site-packages/')
import graph_tool.all as gt

DUMP_LANG = 'en'
if 'WIKI_LANG' in os.environ: DUMP_LANG = os.environ['WIKI_LANG']
DUMP_DATE = '20200901'
if 'WIKI_DATE' in os.environ: DUMP_DATE = os.environ['WIKI_DATE']
RESULTS_DIR = 'results/'

page_file = RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-pages.csv"
edge_file = RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-links-edgelist.csv"
graph_file = RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-graph.gt"

graph = gt.Graph(directed=True)
with open(edge_file) as f:
    graph.vp.id = graph.add_edge_list((tuple(map(int,l.split(' '))) for l in tqdm(f)), hashed=True).copy(value_type='int')

id_index_map = np.zeros((np.max(graph.vp.id.a)+1,), 'int')
id_index_map[graph.vp.id.a] = graph.get_vertices()
graph.vp.title = graph.new_vp('string')
with open(page_file, newline='') as f:
    for row in tqdm(csv.reader(f)):
        graph.vp.title[id_index_map[int(row[0])]] = row[1]
        
graph.save(graph_file)