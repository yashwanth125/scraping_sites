try:
    from selenium import webdriver
    from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
    from datetime import datetime, timedelta,date
    import re
    import json
    from elasticsearch import Elasticsearch
    import os
    import time
    import sys
    import argparse
    from elasticsearch import helpers
    import requests
    import configparser
    import requests
except Exception as e:
    print("Modules Missing {}".format(e))


## This code is used to Scrape the Linux Insights website to TDAP Database
    
# data = {"Linux":"https://insights.lfx.linuxfoundation.org/projects/korg/dashboard;subTab=technical", 
#         "DPDK": "https://insights.lfx.linuxfoundation.org/projects/data-plane-development-kit%2Fdpdk/dashboard;subTab=technical",
#         "EdgeXFoundry": "https://insights.lfx.linuxfoundation.org/projects/lfedge%2Fedgex-foundry/dashboard;subTab=technical",
#         "Yocto": "https://insights.lfx.linuxfoundation.org/projects/yocto/dashboard;subTab=technical",
#         "Zephyr": "https://insights.lfx.linuxfoundation.org/projects/zephyr/dashboard;subTab=technical",
#         "OpenSSF": "https://insights.lfx.linuxfoundation.org/projects/openssf%2Fopenssf/dashboard;subTab=technical"    }

# Set Proxy variables
os.environ["http_proxy"] = "http://proxy-chain.intel.com:911"
os.environ["https_proxy"] = "http://proxy-chain.intel.com:912"


## This Function is used to connect to the Elastic Search

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


## This Function is used to extract the links from Chrome Network Tab using Selenium
def selinum_extractor(url):
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument("--headless")
    chromeOptions.add_argument("--remote-debugging-port=9222")
    chromeOptions.add_argument('--no-sandbox')
    chromeOptions.add_argument('--proxy-server=proxy-chain.intel.com:911')
    chromeOptions.add_argument('--proxy-server=http://proxy-chain.intel.com:912')
    caps = DesiredCapabilities.CHROME
    caps['goog:loggingPrefs'] = {'performance': 'ALL'}
    driver = webdriver.Chrome('/usr/bin/chromedriver',chrome_options=chromeOptions,desired_capabilities=caps)
    driver.get(url)
    
    time.sleep(10) # wait for all the data to arrive. 
    perf = driver.get_log('performance')
    data_list=[]
    for item in perf:
        result=json.loads(item['message'])
        if result['message']['method']=='Network.requestWillBeSent':
            if 'request' in result['message']['params']:
                if re.search('^https://metrics.lfanalytics.io/v1',result['message']['params']['request']['url']):
                    data_list.append(result['message']['params']['request']['url'])
    data_list2=[]
    for item in data_list:
        if 'commit' in item:
            data_list2.append(item)
    for item in data_list:
        if 'pull' in item:
            data_list2.append(item)
    for item in data_list:
        if 'issue' in item:
            data_list2.append(item)
    return data_list2

## This Function is used to Extract the project data
def commit_extractor(repo,url):
    result=requests.get(url).json()
    today_date = date.today()
    response={}
    if len(result)==0:
        return response
    else:
        response['load_date']= today_date
        response['project_name']=repo
        response['repositories']=result['repositories']['count']
        response['sub_projects']=result['projects']['count']
        response['commits_count']=result['commits']['count']
        response['companies']=[item['name'] for item in result['companies']['top_companies']]
        response['top_contributors']=[item['name'] for item in result['contributors']['top_contributors'] ]
        return response



# This Function is used to Extract Company contributions to that Project
def top_companies(repo,url):
    result=requests.get(url).json()
    response=[]
    today_date = date.today()
    if len(result)==0:
        return response
    else:
        for item in result['companies']['top_companies']:
            result={}
            result['_index']='tdap_linux_foundation_top_companies'
            result['_id']=repo+'_'+item['name']+'_'+str(today_date)
            result['load_date']= today_date
            result['project_name']=repo
            result['company_name']=item['name']
            result['commits']=item['commits']
            response.append(result)
        return response

# This Function is used to Extract the Top Contributing Companies 
def top_contributors(repo,url):
    result=requests.get(url).json()
    response=[]
    today_date = date.today()
    if len(result)==0:
        return response
    else:
        for item in result['contributors']['top_contributors']:
            result={}
            result['_index']='tdap_linux_foundation_top_contributors'
            result['_id']=repo+'_'+item['name']+'_'+str(today_date)
            result['load_date']= today_date
            result['project_name']=repo
            result['contributor_name']=item['name']
            result['commits']=item['commits']
            result['lines_of_code']=item['lines_of_code']
            response.append(result)
    return response


## This Function is used to ingest data into ELK Cluster

def ingestor(client,repo,response):
    if len(response) > 0:
      result1 = commit_extractor(repo,response[0])
      result2 = top_companies(repo,response[0])
      result3 = top_contributors(repo,response[0])
      try:
         #res1=helpers.bulk(client,result1)
         print("loading ES")
         result_1 = client.index(index='tdap_linux_foundation_projects', id=repo+'_'+str(date.today()), body=result1)
         result_2=helpers.bulk(client,result2)
         result_3=helpers.bulk(client,result3)
         print('Inserted Repo ' + repo)
      except Exception as e:
          print(e)
    else:
       print('insights disabled for ' + repo)

parser = argparse.ArgumentParser()
parser.add_argument("-es", "--Elasticsearch", help = "specify Elasticsearch environment: (Dev/Prod)" , default='Dev')
args = parser.parse_args()

# error handling in inputs: 
if args.Elasticsearch not in ['Dev','Prod']:
    sys.exit('wrong entry for ES env. please use "Dev" or "Prod".')

ES_Env = configparser.ConfigParser()
ES_Env.read('/home/bduser/linux-foundation/es_env.cfg')
if args.Elasticsearch == 'Prod':
    es_handler = "Elasticsearch_Prod"
else: 
    es_handler = "Elasticsearch_Dev"

es_host = ES_Env.get(es_handler, "es_host").strip('"')
es_port = ES_Env.get(es_handler, "es_port").strip('"')
es_user = ES_Env.get(es_handler, "es_user").strip('"')
es_pass = ES_Env.get(es_handler, "es_pass").strip('"')
es_client = es_connector(es_host, es_port, es_user, es_pass)  




## This is the starting point of the Code

page_link='https://metrics.lfanalytics.io/v1/projects?pageNo=1&pageSize=150'
request = requests.get(page_link)
result = request.json()
extract_link='https://insights.lfx.linuxfoundation.org/projects/%s/dashboard;subTab=technical'
for project in result['projects']:
    print('loading ' + project['slug']+ ' ' +datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    link =extract_link % project['slug']
    print(link)
    response= selinum_extractor(link)
    print('extract done')
    ingestor(es_client,project['slug'],response)