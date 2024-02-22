from airflow import DAG
from airflow.operators.python import PythonOperator

from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
from datetime import datetime as dt
from datetime import timedelta
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from fake_useragent import UserAgent
import random
from lxml import etree
from hdfs import InsecureClient
import pyarrow.parquet as pq
from io import BytesIO
import boto3
import logging

import warnings
warnings.filterwarnings('ignore')



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt(2024, 2, 15),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'CollectNaverNews',
    default_args=default_args,
    description='Car Naver News Collect in Daily',
    schedule_interval="0 0 * * *",
    catchup=False
)

def news_url_collect():
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

    df = pd.DataFrame(news_url_list, columns =['news_url_list'])

    df.to_csv(f"/home/ubuntu/naver_news_list/{day}_car_News_URL.csv", index = None)

news_Url_Collect_task = PythonOperator(
    task_id='news_url_collect',
    python_callable=news_url_collect,
    dag=dag,
)

def news_Content_Comment_Collect():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

    news_idx = 0
    news_comment_idx = 0
    day = (dt.today()-timedelta(days=1)).strftime("%Y%m%d")
    news_df = pd.DataFrame(columns = ("title","url","datetime","content"))
    news_comment_df = pd.DataFrame(columns = ("title","url","datetime","content","c_author","c_datetime","c_content"))
    news_url_list = pd.read_csv(f"/home/ubuntu/naver_news_list/{day}_car_News_URL.csv")
    news_url_list = list(news_url_list['news_url_list'])

    for url in news_url_list:
        driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(),options=options)
        logging.info("접속완료")
        driver.get(url)
        driver.implicitly_wait(10)
        logging.info(f'{news_idx} Start')
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
        logging.info(f'{news_idx} End')
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

news_Content_Comment_Collect_task = PythonOperator(
    task_id='news_Content_Comment_Collect',
    python_callable=news_Content_Comment_Collect,
    dag=dag,
)



#Task 실행순서
news_Url_Collect_task >> news_Content_Comment_Collect_task
