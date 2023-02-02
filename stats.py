from read import *

#isin dictionary
isin_grp_dict = get_all_isin_groups()

file = '../data.Goldman_Sachs.pickle.zip'
arr = load_from_pickle(file)

print(file, ', data len:', len(arr))



#TODO genauer überprüfen, Zusammenfassung möglich?

isin_idx = 0
for isin_data in arr:
    d = isin_data[0]
    extra = isin_data[1]
    
    print('extra:', extra)
    for trade in d:
        print(timestamp_to_strtime(trade[0]), trade)

    isin_idx += 1
    break
    print(isin, ', data len:', len(data))

    if len(data) > 10:
        break;



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