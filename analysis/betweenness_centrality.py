import os
import sys
import pickle
import json
import numpy as np
from datetime import datetime, timedelta

import graph_tool.all as gt

import logging
logging.basicConfig(filename='bc.log', level=logging.DEBUG)

logging.info(f"Started at {datetime.now()}")
logging.info(f"OpenMP: {gt.openmp_enabled()} {gt.openmp_get_num_threads()}")

graph = gt.load_graph("results/enwiki-20201220-graph.gt")
id_index_map = np.zeros((np.max(graph.vp.id.a)+1,), 'int')
id_index_map[graph.vp.id.a] = graph.get_vertices()

logging.info(f"Loaded {graph.num_vertices()} nodes")

largest_component_filter = gt.label_largest_component(graph)
lc_graph = gt.GraphView(graph, largest_component_filter)

logging.info(f"Reduced to {lc_graph.num_vertices()} nodes")

start_time = datetime.now()

vb_map = gt.betweenness(graph, np.random.choice(graph.get_vertices(), 10000, replace=False))[0]
#vb_map.a[np.setdiff1d(graph.get_vertices(), lc_graph.get_vertices(), assume_unique=True)] = -1

end_time = datetime.now()
logging.info(f"Computation took {end_time-start_time}")

with open("results/enwiki-20201220-betweenness-centrality.pkl", "wb") as f:
    pickle.dump(vb_map, f)

bc = sorted([(vb_map[v], graph.vp.title[v]) for v in graph.vertices()])[::-1]

with open(f"results/betweenness-centrality.json", "w") as f:
    json.dump([{"title": t, "value": v} for v,t in bc[:100]], f)