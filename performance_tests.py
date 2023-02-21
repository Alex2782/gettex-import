from utils import *
from isin_groups import *
from convert import *

# Performance Tests
# ----------------------------------------------------------

# isin dictionary
start = timeit.default_timer()
isin_grp_dict = get_all_isin_groups()
groups = get_isin_group_keys()
stop = timeit.default_timer()
print('get_all_isin_groups + get_isin_group_keys: %.2f s' % (stop - start))

# loading group isin
for grp in groups:

    start = timeit.default_timer()
    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(grp)
    isin_dict = isin_grp_dict[grp]['isin_dict']
    isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']
    trade_data = init_trade_data(isin_dict)
    stop = timeit.default_timer()

    print(f'INIT data: isin + trade_data {stop-start:>6.3f} s, LEN: {len(isin_dict):>7}, GROUP: {grp}')


# read gz files
def read_gz(path):

    total = None
    size = get_file_sizeinfo(path)
    size_val = size.replace(' MB', '')
    print (f'FILE: {path}, SIZE: {size}')

    start = timeit.default_timer()
    with gzip.open(path, 'rt') as f:

        idx = 0

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):
            tmp = line.split(',')
            tmp[6] = int(tmp[6])
            #print (line)
            #idx += 1
            #if idx > 10: break
            continue

    stop = timeit.default_timer()
    runtime = stop-start
    print(f'read gz file: {runtime:>6.3f} s, speed: {float(size_val)/runtime:>6.3f} MB/s')

# from ssd
path = '../data_ssd/pretrade.20230112.11.00.mund.csv.gz' #240 MB
print ('FROM SSD')
read_gz(path)

# from hdd
path = '../data/2023-01-12/pretrade.20230112.11.00.mund.csv.gz' #240 MB
#print ('FROM HDD')
#read_gz(path)



#get_all_isin_groups + get_isin_group_keys: 0.24 s
#INIT data: isin + trade_data  0.012 s, LEN:   28431, GROUP: None
#INIT data: isin + trade_data  0.016 s, LEN:   90546, GROUP: HSBC
#INIT data: isin + trade_data  0.040 s, LEN:  197826, GROUP: Goldman_Sachs
#INIT data: isin + trade_data  0.024 s, LEN:  149008, GROUP: UniCredit
    
#get_all_isin_groups + get_isin_group_keys: 0.23 s
#INIT data: isin + trade_data  0.013 s, LEN:   28431, GROUP: None
#INIT data: isin + trade_data  0.016 s, LEN:   90627, GROUP: HSBC
#INIT data: isin + trade_data  0.038 s, LEN:  197843, GROUP: Goldman_Sachs
#INIT data: isin + trade_data  0.024 s, LEN:  149008, GROUP: UniCredit