import os
from os import path
import sys
import requests
import json
import bz2
from multiprocessing import Pool, Manager
from functools import partial
import pickle
import re

with open('index.pkl', 'rb') as f:
    page_ids = pickle.load(f)

print('\n'.join(filter(lambda p : p.startswith('at&t'), pages)))