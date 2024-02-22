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
from datetime import timedelta
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from hdfs import InsecureClient
import pyarrow.parquet as pq
from io import BytesIO
import boto3

import warnings
warnings.filterwarnings('ignore')

ua = UserAgent()  # UserAgent 객체 생성
user_agent = ua.random  # User-Agent 설정

options = Options()
options.add_argument("--headless")
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--window-size=1920,1080")
options.add_argument("disable-gpu")
options.add_argument("lang=ko_KR")
options.add_argument('Content-Type=application/json; charset=utf-8')
options.add_argument(f'user-agent={user_agent}')

#전날 뉴스데이터 수
news_url_list = []
day = (dt.today()-timedelta(days=1)).strftime("%Y%m%d")
url = f'https://news.naver.com/breakingnews/section/103/239?date={day}'
driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),options=options)
driver.get(url)
driver.implicitly_wait(10)

while True:
    try:
        more_btn = driver.find_element(By.XPATH, '//div[@class="section_more"]/a')
        driver.execute_script("arguments[0].click();",more_btn)
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//div[@class="section_more"]/a')))  
    except TimeoutException:
        break

html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')
tree = etree.HTML(str(soup))
hrefs = tree.xpath('//div[@class="section_latest"]/div/div/div/ul/li/descendant::div[@class="sa_text"]/a/@href')
news_url_list.extend(hrefs) 

news_idx = 0
news_comment_idx = 0
news_df = pd.DataFrame(columns = ("title","url","datetime","content"))
news_comment_df = pd.DataFrame(columns = ("title","url","datetime","content","c_author","c_datetime","c_content"))
# news_url_list = pd.read_csv(f"/home/hyunwoo/airflow/naver_news/{today}_IT_News_URL.csv")

for url in news_url_list:
    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),options=options)
    #logging.info("접속완료")
    driver.get(url)
    driver.implicitly_wait(10)
    #logging.info(f'{news_idx} Start')
    #본문수집
    try:
        title = driver.find_element(By.XPATH, '//div[@class="media_end_head_title"]/h2/span').text
        url = url
        datetime = driver.find_element(By.XPATH, '//div[@class="media_end_head_info_datestamp_bunch"]/span').text
        content = driver.find_element(By.XPATH, '//div[@class="newsct_article _article_body"]').text.replace('\n','')
        news_df.loc[news_idx] = [title,url,datetime, content]
        news_idx += 1
        comment_page = driver.find_element(By.XPATH, '//div[@class="media_end_head_info_variety_cmtcount _COMMENT_HIDE"]/a')
        driver.execute_script("arguments[0].click();",comment_page)

        while True:
            try:
                more_btn = driver.find_element(By.XPATH, '//span[@class="u_cbox_page_more"]')
                driver.execute_script("arguments[0].click();",more_btn)
                WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//span[@class="u_cbox_page_more"]')))  
            except TimeoutException:
                break

        comments = driver.find_elements(By.XPATH, '//div[@class="u_cbox_comment_box u_cbox_type_profile"]')

        for comment in comments:
            try:
                c_author = comment.find_element(By.XPATH,'.//span[@class="u_cbox_nick"]').text
                c_datetime = comment.find_element(By.XPATH,'.//span[@class="u_cbox_date"]').text
                c_content = comment.find_element(By.XPATH,'.//span[@class="u_cbox_contents"]').text

                news_comment_df.loc[news_comment_idx] = [title,url,datetime, content, c_author, c_datetime, c_content]
                news_comment_idx += 1
            except NoSuchElementException:
                continue
    except:
        pass
    
    driver.quit()
    #logging.info(f'{news_idx} End')
    time.sleep(random.randint(2,3))

df = pd.merge(news_df,news_comment_df)

parquet_buffer = BytesIO()
df.to_parquet(parquet_buffer, index=False)

# S3에 연결
s3 = boto3.client('s3', aws_access_key_id='AKIA4LH2MAOFAW5PUKUT',
                  aws_secret_access_key='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')

# 버킷 이름과 저장할 파일 경로 설정
bucket_name = 'hyunwoo-toy-project-bucket'
file_path = f'naver_news/{day}_news.parquet'

# 버킷에 Parquet 파일 저장
parquet_buffer.seek(0)
s3.upload_fileobj(parquet_buffer, bucket_name, file_path)
