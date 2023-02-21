from utils import *
from isin_groups import *
from convert import *




# Performance Tests
# ----------------------------------------------------------

#isin dictionary
start = timeit.default_timer()
isin_grp_dict = get_all_isin_groups()
groups = get_isin_group_keys()
stop = timeit.default_timer()
print('get_all_isin_groups + get_isin_group_keys: %.2f s' % (stop - start))

for grp in groups:

    start = timeit.default_timer()
    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(grp)
    isin_dict = isin_grp_dict[grp]['isin_dict']
    isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']
    trade_data = init_trade_data(isin_dict)
    stop = timeit.default_timer()

    print(f'INIT data: isin + trade_data {stop-start:>6.3f} s, LEN: {len(isin_dict):>7}, GROUP: {grp}')

    