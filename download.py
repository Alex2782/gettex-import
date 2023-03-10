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


job_files = {}
for file in files:

    file = file.replace('"', '').replace('&amp;', '&');
    name = os.path.basename(file)

    tmp = name.split('.', 4)
    job_name = tmp[1] + '.' + tmp[2] + '.' + tmp[3]

    if not job_files.get(job_name): job_files[job_name] = []
    job_files[job_name].append(file)



#key sort (job_name)
job_files = dict(sorted(job_files.items(), key=lambda x: x[0]))

# sort files -> post.XYZ.munc -> post.XYZ.mund -> pre.XYZ.munc -> pre.XYZ.mund
# and download files
for job_name in job_files:

    job_files[job_name].sort(key=str.lower)

    if len (job_files[job_name]) != 4:
        print (f'Note: the files for "{job_name}" are incomplete')
    
    name = os.path.basename(job_files[job_name][0])
    date = get_file_date(name)
    output_path = "../data/" + date

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    skip_counter = 0
    for url in job_files[job_name]:
        name = os.path.basename(url)
        
        file_path = output_path + '/' + name

        # check '*.csv.gz' and '*.csv' files, skip if already exist
        # must be valid if exists
        if (os.path.isfile(file_path) and is_valid_gzip(file_path)) or os.path.isfile(file_path[0:-3]):
            skip_counter += 1
            continue

        download_url(url, file_path)
        time.sleep(1)

        #check gz file and try again
        if not is_valid_gzip(file_path):
            time.sleep(2)
            print ('TRY #2: ', file_path)
            download_url(url, file_path)
            time.sleep(2)
            if is_valid_gzip(file_path): print("OK !")
        
        #END for url
    
    if skip_counter == len(job_files[job_name]):
        print (f"SKIPPED: {job_name}")


stop = timeit.default_timer()

show_runtime('files downloaded in', start, stop)   

exit()


#download all files
for url in files:
    url = url.replace('"', '').replace('&amp;', '&');
    name = os.path.basename(url)

    date = get_file_date(name)
    output_path = "../data/" + date

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    output_path += '/' + name

    # check '*.csv.gz' and '*.csv' files, skip if already exist
    # must be valid if exists
    if (os.path.isfile(output_path) and is_valid_gzip(output_path)) or os.path.isfile(output_path[0:-3]):
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


