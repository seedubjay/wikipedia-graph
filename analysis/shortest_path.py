import sys
sys.path.append(sys.path[0] + '/../')
from generate.db import db

def bfs(source, target):
    if db.graph.find_one({"_id": source}) is None or db.graph.find_one({"_id": target}) is None: return None
    prev = {source: None}
    dist = 0
    relax = [source]
    while target not in prev:
        print(dist, len(relax))
        dist += 1
        next_relax = []
        processed = 0
        batchsize = 10000
        while processed < len(relax):
            for page in db.graph.find({"_id": {"$in": relax[processed:min(processed+batchsize,len(relax))]}}):
                s = page['_id']
                l = page['links']
                for e in l:
                    if e not in prev:
                        prev[e] = s
                        next_relax.append(e)
            processed += batchsize
        relax = next_relax
        if len(relax) == 0: return None
    
    ans = []
    x = target
    while x is not None:
        ans.append(x)
        x = prev[x]
    return ans[::-1]

print(bfs(12,25))