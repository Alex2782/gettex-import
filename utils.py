import urllib.request
import json
import re
from tqdm import tqdm

# -------------------------------------------------
# init_request: add header 'User-agent'
# -------------------------------------------------
def init_request():
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler)
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    urllib.request.install_opener(opener)

# -------------------------------------------------
# class DownloadProgressBar
# -------------------------------------------------
class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

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
def get_html(url, log = True):
    if log: print ("get_html: " + url)
    
    html = ''

    try:
        html = urllib.request.urlopen(url).read()
    except Exception as e:
        print(f"Error: {str(e)}, url: {url}")

    return str(html)

# -------------------------------------------------
# get_json
# -------------------------------------------------
def get_json(url, log = True):
    if log: print ("get_json: " + url)
    json_str = urllib.request.urlopen(url).read()
    return json.loads(json_str)


# -------------------------------------------------
# onvista_search
# -------------------------------------------------
def onvista_search(searchValue, website = False, snapshot = False, log = True):

    ret = {}

    url = f'https://api.onvista.de/api/v1/instruments/search/facet?searchValue={searchValue}'
    ret['search'] = get_json(url, log)
    
    instrument = ret['search']['instrument']
    isin = instrument['isin']
    entityType = instrument['entityType']
    entityValue = instrument['entityValue']

    if entityType == 'DERIVATIVE':
        url = f'https://api.onvista.de/api/v1/derivatives/{entityValue}/snapshot'
        ret['derivative_snapshot'] = get_json(url, log)

    # website
    if website:
        url = instrument['urls']['WEBSITE']
        ret['website'] = get_html(url, log)

        start = '<script id="__NEXT_DATA__" type="application/json">'
        end = '</script>'

        match = re.search(f'{start}(.*){end}', ret['website'], re.DOTALL)
        if match:
            data = match.group(1).encode('utf-8').decode('unicode_escape')
            ret['__NEXT_DATA__'] = json.loads(data)

    # snapshot
    if snapshot:
        url = f'https://api.onvista.de/api/v1/instruments/{entityType}/ISIN:{isin}/snapshot'
        ret['snapshot'] = get_json(url, log)

    return ret
