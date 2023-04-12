# DERIVATIVE - GROUPS (ISIN: 'DE000' + XX)
# =============================================
# HSBC: TT, HG, TB, TD, TR, HE
# Goldman Sachs: GZ, GX, GF, GK, GH, GC, GB, GA, GS, GM, CH
# UniCredit: HC, HB, HR, HV, 78, HZ, HX, HW, HU, HY

from utils import *

ISIN_GROUPS = {}
ISIN_GROUPS['HSBC'] = ['DE000TT', 'DE000HG', 'DE000TB', 'DE000TD', 'DE000TR', 'DE000HE']

ISIN_GROUPS['Goldman_Sachs'] = ['DE000GZ', 'DE000GX', 'DE000GF', 'DE000GK', 'DE000GH', 
                                'DE000GC', 'DE000GB', 'DE000GA', 'DE000GS', 'DE000GM', 'DE000CH']

ISIN_GROUPS['UniCredit'] = ['DE000HC', 'DE000HB', 'DE000HR', 'DE000HV', 'DE00078', 
                            'DE000HZ', 'DE000HX', 'DE000HW', 'DE000HU', 'DE000HY']

ISIN_GROUPS_IDX = {}
for grp in ISIN_GROUPS:
    isin_list = ISIN_GROUPS[grp]
    for isin in isin_list:
        ISIN_GROUPS_IDX[isin] = grp

# ---------------------------------------------------------------------------------
# get_isin_group
# ---------------------------------------------------------------------------------
def get_isin_group(isin):
    return ISIN_GROUPS_IDX.get(isin[:7])

# ---------------------------------------------------------------------------------
# get_isin_group_keys
# ---------------------------------------------------------------------------------
def get_isin_group_keys():
    return [None] + list(ISIN_GROUPS.keys())

# ---------------------------------------------------------------------------------
# get_all_isin_groups
# ---------------------------------------------------------------------------------
def get_all_isin_groups():

    groups = get_isin_group_keys()
    ret = {}

    for grp in groups:
        isin_dict, isin_dict_idx = load_isin_dict(grp)
        ret[grp] = {'isin_dict': isin_dict, 'isin_dict_idx': isin_dict_idx}

    return ret


# ---------------------------------------------------------------------------------
# get_isin_path
# ---------------------------------------------------------------------------------
def get_isin_path(group = None):

    path = './isin'
    if not os.path.exists(path): os.makedirs(path)

    filename = 'isin.pickle'
    if group: filename = f'isin.{group}.pickle'

    path += '/' + filename

    return path

# ---------------------------------------------------------------------------------
# save_isin_dict: save isin dictionary as pickle file
# ---------------------------------------------------------------------------------
def save_isin_dict(isin_dict, group = None):

    global _ISIN_SIZE_START
    if not '_ISIN_SIZE_START' in globals(): _ISIN_SIZE_START = {}

    path = get_isin_path(group)

    if _ISIN_SIZE_START.get(group) != len(isin_dict):

        print(f'save ISIN, GRP: {group}, NEW LEN: +{len(isin_dict) - _ISIN_SIZE_START.get(group)} -> {len(isin_dict)}') 
        with open(path, 'wb') as f:
            pickle.dump(isin_dict, f)
        
        _ISIN_SIZE_START[group] = len(isin_dict)
 
# ---------------------------------------------------------------------------------
# load_isin_dict
# ---------------------------------------------------------------------------------
def load_isin_dict(group = None):

    global _ISIN_SIZE_START
    if not '_ISIN_SIZE_START' in globals(): _ISIN_SIZE_START = {}

    path = get_isin_path(group)
    print(f'load_isin_dict: {path:<33}     file size: {get_file_sizeinfo(path):>10}')

    isin_dict = {}

    start = timeit.default_timer()

    if os.path.exists(path):
        with open(path, 'rb') as f:
            isin_dict = pickle.load(f)

    _ISIN_SIZE_START[group] = len(isin_dict)

    stop = timeit.default_timer()
    print(f'loaded isin_dict in: {(stop - start):>6.2f} s          len: {_ISIN_SIZE_START.get(group):>8}     sizeof: {get_sizeof_info(isin_dict):>10}') 

    # isin_dict_idx
    isin_dict_idx = get_isin_dict(isin_dict)

    return isin_dict, isin_dict_idx

# ---------------------------------------------------------------------------------
# get_isin_dict
# ---------------------------------------------------------------------------------
def get_isin_dict(isin_dict):
    isin_dict_idx = {}

    for key, value in isin_dict.items():
        isin_dict_idx[value['id']] = key
    
    return isin_dict_idx
