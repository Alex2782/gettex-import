from utils import *
from isin_groups import *
import time

# ------------------------------------------------------------------------------------
# validate_files
# ------------------------------------------------------------------------------------
def validate_files(path):

    print ("validate_files, path:", path)
    start = timeit.default_timer()

    files = list_gz_files(path, True, True, False)

    invalid_gz_files = []

    for filepath in tqdm(files, unit=' files', unit_scale=True):
        if not is_valid_gzip(filepath, False): invalid_gz_files.append(filepath)

    print('invalid_gz_files len:', len(invalid_gz_files))

    stop = timeit.default_timer()
    show_runtime('Files was checked in in', start, stop)


#invalid_gz_files = []
#filepath = '../data/2023-02-06/pretrade.20230206.14.30.mund.csv.gz'
#
#if not is_valid_gzip(filepath): 
#    if not is_valid_gzip(filepath): invalid_gz_files.append(filepath)
#
#print('invalid_gz_files:', invalid_gz_files)


path = '../data'
validate_files(path)