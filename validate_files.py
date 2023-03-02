from utils import *
from isin_groups import *
from convert import *
import time

# ------------------------------------------------------------------------------------
# validate_files
# ------------------------------------------------------------------------------------
def validate_files(path, validate_gz = False, validate_empty_isin = False):

    print ("validate_files, path:", path)

    isin_grp_dict = get_all_isin_groups()
    groups = get_isin_group_keys()

    start = timeit.default_timer()

    files = list_gz_files(path, True, True, False)

    # validate gzip files
    if validate_gz:
        print ('VALIDATE gzip')
        invalid_gz_files = []

        for filepath in tqdm(files, unit=' files', unit_scale=True):
            if not is_valid_gzip(filepath, False): invalid_gz_files.append(filepath)

        print('invalid_gz_files len:', len(invalid_gz_files))
        pass
    
    sum_volume = 0
    sum_size = 0

    print ('VALIDATE posttrade')
    for filepath in tqdm(files, unit=' files', unit_scale=True):
        name = os.path.basename(filepath)
        
        tmp = name.split('.', 1)
        
        if tmp[0] == 'posttrade':
            with gzip.open(filepath, 'rt') as f:
                for line in f:
                    tmp = line.split(',')
                    isin, tm, seconds, currency, price, amount = cast_data_posttrade(tmp)
                    sum_volume += price * amount
                    sum_size += amount

        elif tmp[0] == 'pretrade':
            pass


    

    files = list_zip_files(path, True, True)
    out_list = []

    volume_grp_stats = {}

    for grp in groups:

        isin_dict = isin_grp_dict[grp]['isin_dict']
        isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']

        print ('GROUP:', grp)

        if volume_grp_stats.get(grp) is None: volume_grp_stats[grp] = {'volume': 0, 'size': 0}

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


            for data in trade:

                if len(data) == 3 and len (data[2]) > 0:
                    post = data[2]

                    for p in post:
                        volume_grp_stats[grp]['volume'] += p[2] * p[3]
                        volume_grp_stats[grp]['size'] += p[3] 


            if validate_empty_isin:
                if 0 <= empty_isin_idx < len(trade) and len(trade[empty_isin_idx]):
                    out_list.append(f'{filepath}\n-->trade: {trade[empty_isin_idx]}')

            pass
        pass

    #debug output
    for line in out_list:
        print (line)

    sum_grp_volume = 0
    sum_grp_size = 0
    for grp in volume_grp_stats:
        volume = volume_grp_stats[grp]['volume']
        size = volume_grp_stats[grp]['size']
        print ('GROUP VOLUME: ', grp, f'{volume: .3f}, size: {size} ')
        sum_grp_volume += volume
        sum_grp_size += size

    print ('-' * 60)
    print (f'SUM GROUP VOLUME    : {sum_grp_volume: .3f}, size: {sum_grp_size} ')
    print (f'SUM POSTTRADE VOLUME: {sum_volume: .3f}, size: {sum_size} ')

    if round(sum_grp_volume, 3) == round(sum_volume, 3) and sum_grp_size == sum_size:
        print('VOLUME DATA: OK !')  

    stop = timeit.default_timer()
    show_runtime('files was checked in', start, stop)


#invalid_gz_files = []
#filepath = '../data/2023-02-06/pretrade.20230206.14.30.mund.csv.gz'
#
#if not is_valid_gzip(filepath): 
#    if not is_valid_gzip(filepath): invalid_gz_files.append(filepath)
#
#print('invalid_gz_files:', invalid_gz_files)

sub_path = '/2023-02-14'

path = '../data'
validate_files(path + sub_path)
#trade = load_from_pickle('../data/2023-02-14/trade.20230214.14.00.pickle.zip')
#print (trade[28545])
#print (timestamp_to_strtime(trade[28545][0][0][0]), pretrade_list_to_dict(trade[28545][0][0]))
#pretrade_debug('../data/2023-02-14/pretrade.20230214.14.00.mund.csv.gz', None, '13:55:20.6896', False)


#TODO performance check
#path = "../data_ssd"
#validate_files(path + sub_path)

