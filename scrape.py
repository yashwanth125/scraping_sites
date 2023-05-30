import requests,lxml
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import os
import chromedriver_binary
import time
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd


final_data =[]

def helper(url,indicator):
    print(url)
    data = {}
    if indicator=='a':
        driver = webdriver.Chrome()
        driver.get(url)
        time.sleep(10) 
        perf_text = driver.page_source
    else:
        perf_text= requests.request('GET',url)
        perf_text = perf_text.text
    soup = BeautifulSoup(perf_text, 'html.parser')
    car_check = soup.find_all('div', attrs={'class':'notification'})
    if len(car_check)>0 and indicator=='n':
        print(car_check)
        print('exited')
        pass
    elif len(car_check)==2 and indicator=='a':
        print(car_check)
        print('exited')
        pass
    else:
        model = soup.find('h1', attrs={'class': 'heading__title'}).text.strip()
        model_variant = soup.find('p', attrs={'class': 'heading__sub-title'}).text.strip()
        year = model_variant.split()[-1]
        price = soup.find('p', attrs={'class': 'price__cost'}).find('span').text.strip()
        features1 = [item.text.strip() for item in soup.find_all('span', attrs={'class': 'key-features__text'})]

        if indicator=='a':
            photo_link = re.findall(r'koComponents.mediaImage(.*?),', perf_text, re.M)[0]
            mobile = soup.find('a', attrs={'class': 'details__call-number InfinityNumber'}).text.strip()
            transmission_type = features1[1]
            fuel_type= features1[3]
            body_type= features1[0]
            mileage = features1[2]
        else:
            photo_link = re.findall(r'koComponents.image(.*?),', perf_text, re.M)[0]
            mobile = soup.find_all('p', attrs={'class': 'details__call'})[2].find('a').text.strip()
            transmission_type = features1[2]
            fuel_type= features1[3]
            body_type= features1[0]
            mileage = features1[1]

        data['URL']=url
        data['Model']=model
        data['Model_variant']=model_variant
        data['Year']=year
        data['Price']=price
        data['Dealer_telephone']=mobile
        data['Photo_link']=photo_link[1:]
        data['Transmission_type']=transmission_type
        data['Fuel_type']=fuel_type
        data['Body_type']=body_type
        data['Mileage']=mileage
        print(data)
        final_data.append(data)
        


url ='https://www.evanshalshaw.com/search/used/cars/'

driver = webdriver.Chrome()
driver.get(url)
time.sleep(10) 
perf_text = driver.page_source

soup = BeautifulSoup(perf_text, 'html.parser')
cars = soup.find_all('h3', attrs={'class': 'search-results__title'})
cars_list, link_list =[],[]
for tag in cars:
    cars_list.append(tag.text.strip())
links = soup.find_all('a', attrs={'class': 'search-results__item-link'})
for tag in links:
    link_list.append(tag['href'])

print(len(link_list))
for i in range(len(link_list)):
    print(i)
    link = link_list[i]
    if link[0] !='h':
        link= 'https://www.evanshalshaw.com/'+link
        helper(link,'a')
    #print(link)
    else:
        helper(link,'n')
    
print(final_data)
df = pd.DataFrame(final_data)
print(df.head())
df.to_csv('data.csv')

