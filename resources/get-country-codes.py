import re
import pickle

d = {}

pattern = re.compile(r"{{(.+?)}}")

with open('resources/country-codes-raw.csv') as f:
    for l in f:
        p,c = map(lambda i: i.strip(), l.split(',', 1))
        for i in pattern.findall(c):
            d[i] = p

with open('resources/country-codes.pkl', 'wb') as f:
    pickle.dump(d,f)