from bs4 import BeautifulSoup
import pandas as pd
import requests
from selenium import webdriver
import time
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import random
from webdriver_manager.chrome import ChromeDriverManager
from lxml import etree
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime as dt
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from hdfs import InsecureClient
import pyarrow.parquet as pq
from io import BytesIO
import boto3

import warnings
warnings.filterwarnings('ignore')

k_car_url_list = []
f_car_url_list = []
today = dt.today().strftime("%Y%m%d")
ua = UserAgent()  # UserAgent 객체 생성
user_agent = ua.random  # User-Agent 설정
headers = {"User-Agent" : user_agent}

options = Options()
options.add_argument("--headless")
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--window-size=1920,1080")
options.add_argument("disable-gpu")
options.add_argument("lang=ko_KR")
options.add_argument('Content-Type=application/json; charset=utf-8')
options.add_argument(f'user-agent={user_agent}')

#url_collect
url = 'https://www.bobaedream.co.kr/list?code=import'
response = requests.get(url,headers = headers)
html = response.text
soup = BeautifulSoup(html, 'html.parser')
tree = etree.HTML(str(soup))
hrefs = tree.xpath('//table[@id="boardlist"]/tbody/tr[not(@class="best")]/td[@class="pl14"]/a[@class="bsubject"]/@href')
f_car_url_list.extend(hrefs)
f_car_url_list = ['https://www.bobaedream.co.kr'+x for x in f_car_url_list]
time.sleep(random.randint(1,2))
del url

url = 'https://www.bobaedream.co.kr/list?code=national'
response = requests.get(url,headers = headers)
html = response.text
soup = BeautifulSoup(html, 'html.parser')
tree = etree.HTML(str(soup))
hrefs = tree.xpath('//table[@id="boardlist"]/tbody/tr[not(@class="best")]/td[@class="pl14"]/a[@class="bsubject"]/@href')
k_car_url_list.extend(hrefs)
k_car_url_list = ['https://www.bobaedream.co.kr'+x for x in k_car_url_list]
time.sleep(random.randint(1,2))

community = k_car_url_list + f_car_url_list

community_idx = 0
community_comment_idx = 0
community_df = pd.DataFrame(columns = ("title","url","datetime","content"))
community_comment_df = pd.DataFrame(columns = ("title","url","datetime","content","c_author","c_datetime","c_content"))

for url in community:
    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),options=options)
    driver.get(url)
    driver.implicitly_wait(10)
    #본문수집
    try:
        title = driver.find_element(By.XPATH, '//div[@class="writerProfile"]/dl/dt').get_attribute("title")
        url = url
        datetime = driver.find_element(By.XPATH, '//div[@class="writerProfile"]/descendant::span[@class="countGroup"]').text
        datetime = datetime.split("|")[-1].strip() 
        content = driver.find_element(By.XPATH, '//div[@class="bodyCont"]').text.replace('\n','')
        community_df.loc[community_idx] = [title,url,datetime, content]
        community_idx += 1


        comments = driver.find_elements(By.XPATH, '//div[@class="commenticontype"]/div/ul[@class="basiclist"]/li')

        for comment in comments:
            try:
                c_author = comment.find_element(By.XPATH,'.//span[@class="name"]').text
                c_datetime = comment.find_element(By.XPATH,'.//span[@class="date"]').text
                c_content = comment.find_element(By.XPATH,'.//dl/dd').text

                community_comment_df.loc[community_comment_idx] = [title,url,datetime, content, c_author, c_datetime, c_content]
                community_comment_idx += 1
            except NoSuchElementException:
                continue
    except:
        pass
    
    driver.quit()
    time.sleep(random.randint(2,3))


df = pd.merge(community_df,community_comment_df)

parquet_buffer = BytesIO()
df.to_parquet(parquet_buffer, index=False)

# S3에 연결
s3 = boto3.client('s3', aws_access_key_id=aws_access_key,
                  aws_secret_access_key=aws_secret_access_key)

# 버킷 이름과 저장할 파일 경로 설정
bucket_name = 'hyunwoo-toy-project-bucket'
file_path = f'community/{today}_community.parquet'

# 버킷에 Parquet 파일 저장
parquet_buffer.seek(0)
s3.upload_fileobj(parquet_buffer, bucket_name, file_path)
