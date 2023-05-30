
try:
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers
    from datetime import datetime, timedelta,date
    import os
    import sys
    import argparse
    import requests
    import configparser
except Exception as e:
    print("Modules Missing {}".format(e))

## This code is used to load dockerhub Repositories

# Set Proxy variables
os.environ["http_proxy"] = "http://proxy-chain.intel.com:911"
os.environ["https_proxy"] = "http://proxy-chain.intel.com:912"

def es_connector(es_host, es_port, es_user, es_pass):
    #client = Elasticsearch(
    #    [es_host],
    #    basic_auth=(es_user, es_pass),
    #    timeout=100
    #)

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


## This Function is used to ingest the data to ELK
def ingestor(query,client,results):
    today_date = date.today()
    for result in results:
        if es_client.indices.exists(index='tdap_dockerhub'):
            query_result = word_checker(es_client,result['repo_name'],'tdap_dockerhub')
            if query_result==1:
                print('Already loaded')
                break
            else:
                pass
        result['_id']=result['repo_name']+'_'+str(today_date)
        result['_index']='tdap_dockerhub'
        result['repo_url']='https://hub.docker.com/r/'+result['repo_name']
        result['doc_text']=result['short_description']
        if (result['repo_name'].find('/') != -1):
            print('found')
            repo_owner, x = result['repo_name'].split('/')
            result['repo_owner']=repo_owner
        else:
            result['repo_owner']="Docker Official Image"
            official_image_request = requests.get('https://hub.docker.com/v2/repositories/library/'+result['repo_name'])
            if official_image_request.status_code == 404:
                pass
            else:
                official_image_result = official_image_request.json()
                del result['star_count']
                if official_image_result['star_count']:
                    result['star_count']= official_image_result['star_count']
                else:
                    result['star_count'] = 0
        result['term']=query
        del result['short_description']
        result['load_date']=today_date
    res=helpers.bulk(client,results)
    #print("working")
    
# This Function is used to Extract the Documents
def helper(query,link,count):
    if count==101:
        return
    print('Extracting Page, '+str(count))
    request = requests.get(link)
    result = request.json()
    ingestor(query,es_client,result['results'])
    if result['next']:
        count += 1
        helper(query,result['next'],count)
    
        

parser = argparse.ArgumentParser()
parser.add_argument("-es", "--Elasticsearch", help = "specify Elasticsearch environment: (Dev/Prod)" , default='Dev')
parser.add_argument("-t", "--Terms", help = "File with specific terms to Load" , default='/home/bduser/dockerhub/docker_terms.txt')
args = parser.parse_args()

# error handling in inputs: 
if args.Elasticsearch not in ['Dev','Prod']:
    sys.exit('wrong entry for ES env. please use "Dev" or "Prod".')

#todo: error trap docker_terms file exists

ES_Env = configparser.ConfigParser()
ES_Env.read('/home/bduser/dockerhub/es_env.cfg')
if args.Elasticsearch == 'Prod':
    es_handler = "Elasticsearch_Prod"
else: 
    es_handler = "Elasticsearch_Dev"

es_host = ES_Env.get(es_handler, "es_host").strip('"')
es_port = ES_Env.get(es_handler, "es_port").strip('"')
es_user = ES_Env.get(es_handler, "es_user").strip('"')
es_pass = ES_Env.get(es_handler, "es_pass").strip('"')
es_client = es_connector(es_host, es_port, es_user, es_pass)



#This Function is used to check the word is present Today
def word_checker(es,word,indx):
    if es.indices.exists(index=indx):
        q={
        "query": 
        {
            "bool": {
            "must": [
                {
                "range": {
                    "load_date": {
                    "gte": "now/d",
                    "lt": "now+1d/d"
                    }
                }
                },
                {
                "match_phrase": {
                    "repo_name":word
                }
                }
            ]
            }
        }
        }
        doc = es.search(index=indx,body=q,size=1)
        if len(doc['hits']['hits'])>0:
            return 1
        else:
            return 0
    else:
        return 0


## This is the Starting point of the Code
with open(args.Terms) as f:
    words = [line.rstrip('\n') for line in f]
    for word in words:
        print(word+' '+datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        if es_client.indices.exists(index='tdap_dockerhub'):
            query_result = word_checker(es_client,word,'tdap_dockerhub')
            if query_result==1:
                print('Already loaded')
            else:
                helper(word,'https://hub.docker.com/v2/search/repositories/?page=1&query='+word,1)
        else:
            helper(word,'https://hub.docker.com/v2/search/repositories/?page=1&query='+word,1)




