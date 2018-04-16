import pandas as pd
import json
import hashlib
from src import googlesearch
import os
import logging

logging.basicConfig(filename='../crawl.log',
    format="%(asctime)s %(levelname)s %(module)s %(message)s",
    level=10)

logger = logging.getLogger(__name__)
df_y = pd.read_csv(open('../../data/y.txt'))
df_y = df_y[df_y.y.str.contains('>')==True]
logger.debug("topics loaded. shape:{}".format(df_y.shape))

data_dir = '../../data/topics_data/'
d = {}
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36'
for index, row in df_y[18:].iterrows():
    query = row['y']
    query = query.replace('>',' ')
    cur_dir = row['y'].replace(',','').replace('> ','__').replace(' ','_')
    for url in googlesearch.search(query, num=10,stop=100, pause=10.0, user_agent=USER_AGENT):
        logger.debug("query:{},url: {}".format(query, url))
        dir_our = os.path.join(data_dir,cur_dir)
        os.makedirs(dir_our, exist_ok=True)
        fn = os.path.join(dir_our, hashlib.md5(url.encode('utf8')).hexdigest())
        d[fn]=url
        if not os.path.exists(fn):
            with open(fn,'wb') as out_html:
                try:
                    out_html.write(googlesearch.get_page(url,user_agent=googlesearch.get_random_user_agent()))
                except Exception as e:
                    logger.exception(e,url)
    json.dump(d,open('{}.kv'.format(query),'w'))