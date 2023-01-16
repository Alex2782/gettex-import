import gzip

with gzip.open('../data/posttrade.20230111.21.00.munc.csv.gz', 'rb') as f:

    file_content = f.readline()

    while file_content:
        print(file_content)
        file_content = f.readline()
        

exit(0)
#=================================================================================
print("----------------------")

with gzip.open('../data/pretrade.20230111.21.00.munc.csv.gz', 'rb') as f:

    file_content = f.readline()

    while file_content:
        print(file_content)
        file_content = f.readline()
        