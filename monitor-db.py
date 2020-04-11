from pymongo import MongoClient
import time

DUMP_LANG = 'pt'
DUMP_DATE = '20200401'

db_client = MongoClient('localhost', 27017)
db = db_client[f"wikipedia-{DUMP_LANG}wiki-{DUMP_DATE}"]
file_db = db.uploaded_files

def page_count():
    return list(file_db.aggregate([{'$group': {'_id': None, 'total': {'$sum': '$page_count'}}}]))[0]['total']

start_time = time.time()
start_count = page_count()

print()
print()
while True:
    time.sleep(.2)
    print("\033[F\033[F", end='')
    print(f"{round((page_count()-start_count)/(time.time()-start_time))} pages/s")
    print(f"{page_count()} pages")
