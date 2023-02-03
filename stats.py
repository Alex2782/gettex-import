from read import *

#isin dictionary
isin_grp_dict = get_all_isin_groups()

file = '../data.Goldman_Sachs.pickle.zip'
arr = load_from_pickle(file)

print(file, ', data len:', len(arr))



#TODO: genauer überprüfen, Zusammenfassung möglich?

isin_dict = isin_grp_dict['Goldman_Sachs']['isin_dict']
isin_dict_idx = isin_grp_dict['Goldman_Sachs']['isin_dict_idx']

isin_idx = 0
max_vola_long = 0
max_vola_short = 0

for isin_data in arr:
    d = isin_data[0]
    extra = isin_data[1]

    print('isin:', isin_dict_idx[isin_idx])    
    print('extra:', extra)

    sum_vola_long = 0
    sum_vola_short = 0
    sum_vola_open_close_long = 0
    sum_vola_open_close_short = 0

    prev_trade = False
    for trade in d:
        print(timestamp_to_strtime(trade[0]), trade)
        sum_vola_long += trade[12]
        sum_vola_short += trade[13]

        open = trade[7]
        close = trade[10]

        if prev_trade :
            vola =  round(open - prev_trade[10], 3)
            if vola > 0: sum_vola_open_close_long += vola
            else: sum_vola_open_close_short += vola

        prev_trade = trade
    
    print('----------------------------------------------')
    sum_vola_long = round(sum_vola_long, 3)
    sum_vola_short = round(sum_vola_short, 3)
    sum_vola_open_close_long = round(sum_vola_open_close_long, 3)
    sum_vola_open_close_short = round(sum_vola_open_close_short, 3)
    
    print('sum_vola long / short:', sum_vola_long,  sum_vola_short)
    print('sum_vola_open_close long / short:', sum_vola_open_close_long, sum_vola_open_close_short)

    sum_long = round(sum_vola_open_close_long + sum_vola_long, 3)
    print('sum_long:', sum_long)

    sum_short = round(sum_vola_open_close_short + sum_vola_short, 3)
    print('sum_short:', sum_short)

    sum = round(sum_vola_open_close_long + sum_vola_long + sum_vola_open_close_short + sum_vola_short, 3)
    print('sum:', sum)

    isin_idx += 1

    if isin_idx > 2: break
    




exit()
# Maximalen Wert aus der Spalte 'bid_size' ermitteln
max_bid_size = np.amax(x['bid_size'])
print("Maximaler Wert in bid_size:", max_bid_size)

# Maximalen Wert aus der Spalte 'ask_size' ermitteln
max_ask_size = np.amax(x['ask_size'])
print("Maximaler Wert in ask_size:", max_ask_size)


# Maximalen Wert aus der Spalte 'bid' ermitteln
max_bid_size = np.amax(x['bid'])
print("Maximaler Wert in bid:", max_bid_size)

# Maximalen Wert aus der Spalte 'ask' ermitteln
max_ask_size = np.amax(x['ask'])
print("Maximaler Wert in ask:", max_ask_size)


max_bid_index = np.argmax(x['bid'])
isin_idx = x[max_bid_index]['isin_idx']
print("Maximaler Wert in bid:", x[max_bid_index]['bid'], 'isin_idx:', isin_idx)
print("ISIN:", isin_dict_idx.get(isin_idx))

max_ask_index = np.argmax(x['ask'])
isin_idx = x[max_ask_index]['isin_idx']
print("Maximaler Wert in ask:", x[max_ask_index]['ask'], 'isin_idx:', isin_idx)
print("ISIN:", isin_dict_idx.get(isin_idx))

indices = np.where(x['isin_idx'] == 44)
print('data isin_idx = 44:', x[indices])