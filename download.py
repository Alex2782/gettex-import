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

# -------------------------------------------------
# get_job_name_and_encode_filename
# -------------------------------------------------
def get_job_name_and_encode_filename(file):
    file = file.replace('"', '').replace('&amp;', '&');
    name = os.path.basename(file)

    tmp = name.split('.', 4)
    job_name = tmp[1] + '.' + tmp[2] + '.' + tmp[3]

    return file, job_name
# =======================================================================================================

start = timeit.default_timer()

init_request() # init 'User-agent' for all requests

data_url = "https://www.gettex.de/handel/delayed-data/"

# get pretrade and posttrade file URLs
# check files, try again if not match
for t in range(3):
    files = []
    complete = False
    for url_path in ("pretrade-data/", "posttrade-data/"):

        html = get_html(data_url + url_path)
        tmp = find_files(html)

        if len(files) > 0:
            file1 = files[0]
            file2 = tmp[0]
            file1, job_name1 = get_job_name_and_encode_filename(file1)
            file2, job_name2 = get_job_name_and_encode_filename(file2)

            print('check pre/post-files:', job_name1, '<->', job_name2)
            if job_name1 != job_name2:
                print ('files are incomplete, TRY again in 10 seconds ...')
                time.sleep(10)
                break        
        
        if len (files) > 0: complete = True

        files += tmp
    #END for url_path
    if complete: break

print ("number of files: ", len(files))

job_files = {}
for file in files:

    file, job_name = get_job_name_and_encode_filename(file)

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
            time.sleep(5)
            print ('TRY #2: ', file_path)
            download_url(url, file_path)
            time.sleep(5)
            print ('TRY #3: ', file_path)
            download_url(url, file_path)
            if is_valid_gzip(file_path): print("OK !")
            else: 
                print(f'ERROR: file {file_path} is not valid and will be removed')
                os.remove(file_path)
        
        #END for url
    
    if skip_counter == len(job_files[job_name]):
        print (f"SKIPPED: {job_name}")


stop = timeit.default_timer()

show_runtime('files downloaded in', start, stop)
