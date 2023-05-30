try:
    from bs4 import BeautifulSoup
    import os # for proxy
    import requests, lxml
    import json
    import configparser
    import argparse
    import sys
    from os.path import exists
    from datetime import date
    import re
    from elasticsearch import Elasticsearch
    
except Exception as e:
    print("Modules Missing {}".format(e))

headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.3"
}

# Set Proxy variables
os.environ["http_proxy"] = "http://proxy-chain.intel.com:911"
os.environ["https_proxy"] = "http://proxy-chain.intel.com:912"


def es_connector(es_host, es_port, es_user, es_pass):
    # connector to ELK as 02042021
    client = Elasticsearch(
        [es_host],
        http_auth=(es_user, es_pass),
        scheme="http",
        port=es_port,
    #     ssl_context=context,
        timeout=100
    )
    if client.ping():
        print("Connected to ES\n")
        return client
    else:
        sys.exit("Failed connection to ES")
        return None

def Googleloader(word, es_client):
    elk_res={}
    today_date = date.today()
    html = requests.request('GET','https://www.google.com/search?q='+word, headers=headers)
    soup = BeautifulSoup(html.text, 'lxml')
    number_of_results = soup.select_one('#result-stats nobr').previous_sibling
    result = re.sub(r'[a-zA-Z ,]', '',number_of_results)
    elk_res['doc_date']=today_date
    elk_res['load_date']=today_date
    elk_res['count']=int(result)
    elk_res['term']=word
    try:
        response = es_client.index(index="tdap_google_search",document=elk_res, id=word+str(today_date))
        print('working on ' + word)
    except Exception as e:
        print(e)

parser = argparse.ArgumentParser()
parser.add_argument("-es", "--Elasticsearch", help = "specify Elasticsearch environment: (Dev/Prod)" , default='Prod')
parser.add_argument("-cfg", "--Config", help = "specify terms config file" )

args = parser.parse_args()

# error handling in inputs: 
if args.Elasticsearch not in ['Dev','Prod']:
    sys.exit('wrong entry for ES env. please use "Dev" or "Prod".')
if not exists(args.Config):
    sys.exit('config file does not exist!')

ES_Env = configparser.ConfigParser()
ES_Env.read('/home/bduser/google/es_env.cfg')
if args.Elasticsearch == 'Prod':
    es_handler = "Elasticsearch_Prod"
else: 
    es_handler = "Elasticsearch_Dev"
es_host = ES_Env.get(es_handler, "es_host").strip('"')
es_port = ES_Env.get(es_handler, "es_port").strip('"')
es_user = ES_Env.get(es_handler, "es_user").strip('"')
es_pass = ES_Env.get(es_handler, "es_pass").strip('"')

es_client = es_connector(es_host, es_port, es_user, es_pass)


with open(args.Config) as f:
    terms = [line.rstrip('\n') for line in f]
    for term in terms:
        a = Googleloader(term, es_client)
