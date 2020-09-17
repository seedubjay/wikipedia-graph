import os
import sys
from tqdm import tqdm
import numpy as np
import csv
import pickle
import subprocess
import threading

# DUMP_LANG = 'en'
# DUMP_DATE = '20200401'
# RESULTS_DIR = 'results/'
# page_file = RESULTS_DIR + f"{DUMP_LANG}wiki-{DUMP_DATE}-pages.csv"

# title_id_map = {}
# id_title_map = {}

# with open(page_file, newline='') as f:
#     for row in tqdm(csv.reader(f)):
#         title_id_map[row[1]] = int(row[0])
#         id_title_map[int(row[0])] = row[1]

with subprocess.Popen(['./path'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stdout, universal_newlines=True) as process:
    with tqdm() as pbar:
        p = 0
        while True:
            output = process.stdout.readline().strip()
            if output == 'ready': break
            pbar.update(int(output)-p)
            p = int(output)
    print('ready')
    process.stdin.write('1 2 \n')
    print(process.poll())
    while True:
        print(process.stdout.read())
    while True:
        a,b = map(int,input().split())
        process.stdin.write(f"{a} {b}\n")
        print('written')
        print(process.stdout.readline())