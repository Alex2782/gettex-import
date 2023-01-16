import urllib.request
import re
import os.path

from tqdm import tqdm

# -------------------------------------------------
# class DownloadProgressBar
# -------------------------------------------------
class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

# -------------------------------------------------
# init_request: add header 'User-agent'
# -------------------------------------------------
def init_request():
    opener = urllib.request.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    urllib.request.install_opener(opener)

# -------------------------------------------------
# download_url
# -------------------------------------------------
def download_url(url, output_path):

    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=url.split('/')[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)

# -------------------------------------------------
# get_html
# -------------------------------------------------
def get_html(url):
    print ("get_html: " + url)
    html = urllib.request.urlopen(url).read()
    return str(html)

# -------------------------------------------------
# find_files
# -------------------------------------------------
def find_files(html):

    files = re.findall(r'(https?://erdk.bayerische-boerse.de[^\s]+)', html)
    return files

# =======================================================================================================


init_request() # init 'User-agent' for all requests

data_url = "https://www.gettex.de/handel/delayed-data/"

# get pretrade and posttrade
files = []

for url_path in ("pretrade-data/", "posttrade-data/"):

    html = get_html(data_url + url_path)
    files += find_files(html)

print ("count files: ", len(files))

for url in files:
    url = url.replace('"', '').replace('&amp;', '&');
    name = os.path.basename(url)

    output_path = "../data/" + name

    #check '*.csv.gz' and '*.csv' files
    if os.path.isfile(output_path) or os.path.isfile(output_path[0:-3]):
        print ("SKIP: " + output_path)
        continue 
    
    download_url(url, output_path)



# TODO Stammdaten
# https://mein.finanzen-zero.net/assets/searchdata/downloadable-instruments.csv


