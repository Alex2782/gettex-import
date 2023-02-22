from utils import *
import time
import re
import os.path
from datetime import datetime



# -------------------------------------------------
# find_files
# -------------------------------------------------
def find_files(html):

    files = re.findall(r'(https?://erdk.bayerische-boerse.de[^\s]+)', html)
    return files

# -------------------------------------------------
# get_file_date
# -------------------------------------------------
def get_file_date(name):

    arr = name.split('.', 2)
    file_date = arr[1]
    str_date = file_date[0:4] + '-' + file_date[4:6] + '-' + file_date[6:8]

    return str_date

# =======================================================================================================

start = timeit.default_timer()

init_request() # init 'User-agent' for all requests

data_url = "https://www.gettex.de/handel/delayed-data/"

# get pretrade and posttrade file URLs
files = []

for url_path in ("pretrade-data/", "posttrade-data/"):

    html = get_html(data_url + url_path)
    files += find_files(html)

print ("count files: ", len(files))

#download all files
for url in files:
    url = url.replace('"', '').replace('&amp;', '&');
    name = os.path.basename(url)

    date = get_file_date(name)
    output_path = "../data/" + date

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    output_path += '/' + name

    #check '*.csv.gz' and '*.csv' files, skip if already exist
    if os.path.isfile(output_path) or os.path.isfile(output_path[0:-3]):
        print ("SKIP: " + output_path)
        continue 
    
    download_url(url, output_path)
    time.sleep(1)

    #check gz file and try again
    if not is_valid_gzip(output_path):
        time.sleep(2)
        print ('TRY #2: ', output_path)
        download_url(url, output_path)
        time.sleep(2)
        if is_valid_gzip(output_path): print("OK !")

stop = timeit.default_timer()

show_runtime('files downloaded in', start, stop)

# TODO: main data
# https://mein.finanzen-zero.net/assets/searchdata/downloadable-instruments.csv


