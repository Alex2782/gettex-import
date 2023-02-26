from utils import *
from isin_groups import *
from convert import get_all_isin_groups, get_isin_group_keys, pretrade_debug, pretrade_list_to_dict
import time

# ------------------------------------------------------------------------------------
# validate_files
# ------------------------------------------------------------------------------------
def validate_files(path, validate_gz = False, validate_empty_isin = False):

    print ("validate_files, path:", path)
    start = timeit.default_timer()

    isin_grp_dict = get_all_isin_groups()
    groups = get_isin_group_keys()

    # validate gzip files
    if validate_gz:
        files = list_gz_files(path, True, True, False)

        invalid_gz_files = []

        for filepath in tqdm(files, unit=' files', unit_scale=True):
            if not is_valid_gzip(filepath, False): invalid_gz_files.append(filepath)

        print('invalid_gz_files len:', len(invalid_gz_files))
        pass


    files = list_zip_files(path, True, True)
    out_list = []

    for grp in groups:

        isin_dict = isin_grp_dict[grp]['isin_dict']
        isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']

        print ('GROUP:', grp)

        if validate_empty_isin:
            empty_isin = isin_dict.get('')
            if not empty_isin: continue
            print ('empty isin', empty_isin)
            empty_isin_idx = empty_isin['id']

        for filepath in tqdm(files, unit=' files', unit_scale=True):
            
            basename = os.path.basename(filepath)
            tmp = basename.split('.')

            file_date = tmp[1]
            file_grp = tmp[4]
            
            if file_grp == 'pickle': file_grp = None
            if grp != file_grp: continue

            trade = load_from_pickle(filepath)

            if validate_empty_isin:
                if 0 <= empty_isin_idx < len(trade) and len(trade[empty_isin_idx]):
                    out_list.append(f'{filepath}\n-->trade: {trade[empty_isin_idx]}')

            pass
        pass

    #debug output
    for line in out_list:
        print (line)

    stop = timeit.default_timer()
    show_runtime('files was checked in', start, stop)


#invalid_gz_files = []
#filepath = '../data/2023-02-06/pretrade.20230206.14.30.mund.csv.gz'
#
#if not is_valid_gzip(filepath): 
#    if not is_valid_gzip(filepath): invalid_gz_files.append(filepath)
#
#print('invalid_gz_files:', invalid_gz_files)

path = '../data'
path = '../data/2023-02-14'
validate_files(path)
#trade = load_from_pickle('../data/2023-02-14/trade.20230214.14.00.pickle.zip')
#print (trade[28545])
#print (timestamp_to_strtime(trade[28545][0][0][0]), pretrade_list_to_dict(trade[28545][0][0]))
#pretrade_debug('../data/2023-02-14/pretrade.20230214.14.00.mund.csv.gz', None, '13:55:20.6896', False)
