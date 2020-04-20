import json
import sys

def remove_outputs(fn):
    with open(fn) as f:
        d = json.load(f)
    for (i,cell) in enumerate(d['cells']):
        d['cells'][i]['outputs'] = []
    with open(fn,'w') as f:
        json.dump(d,f, indent=1)

if __name__ == '__main__':
    fname = sys.argv[1]
    remove_outputs(fname)