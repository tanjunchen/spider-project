import requests
import re
import pymongo
import random
import time
import json
import random
import numpy as np
import csv
import pandas as pd
from fake_useragent import UserAgent
import socket  # 断线重试
from urllib.parse import urlencode

# 随机ua
ua = UserAgent()

client = pymongo.MongoClient('localhost', 27017)
# 获得数据库
db = client.ITJUZI
mongodb_collection_company = db.itjuzi_company


class ITJUZI(object):
    def __init__(self):
        self.headers = {
            'User-Agent': ua.random,
            'X-Requested-With': 'XMLHttpRequest',
            # 主页cookie
            'Cookie': '76b20f6015442399597225100e18094750f575673b045da6ec0b77984422f6',
        }
        self.url = 'https://www.itjuzi.com/api/companys'  # company
        self.session = requests.Session()

    def get_table(self, page):

        company_payload = {"pagetotal": 121292, "total": 0, "per_page": 30, "scope": "", "sub_scope": "",
                           "round": "", "location": "", "prov": "", "city": "", "status": "", "sort": "",
                           "selected": ""}
        retrytimes = 3
        while retrytimes:
            try:
                response = self.session.get(
                    self.url, params=company_payload, headers=self.headers, timeout=(5, 20)).json()
                print(response)
                self.save_to_mongo(response)
                break
            except socket.timeout:
                print('下载第{}页，第{}次网页请求超时'.format(page, retrytimes))
                retrytimes -= 1

    def save_to_mongo(self, response):
        try:

            data = response['data']['data']
            df = pd.DataFrame(data)
            table = json.loads(df.T.to_json()).values()
            if mongodb_collection_company.insert_many(table):  # investevent
                # if mongo_collection2.insert_many(table):    # company
                # if mongo_collection3.insert_many(table):    # investment
                # if mongo_collection4.insert_many(table):    # horse
                print('存储到mongodb成功')
                sleep = np.random.randint(3, 7)
                time.sleep(sleep)
        except Exception as e:
            print('存储到mongodb失败', e)

    def spider_itjuzi(self, start_page, end_page):
        for page in range(start_page, end_page):
            print('下载第%s页:' % page)
            self.get_table(page)

        print('下载完成')


if __name__ == '__main__':
    spider = ITJUZI()
    spider.spider_itjuzi(398, 4045)
