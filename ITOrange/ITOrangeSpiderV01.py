#!/usr/bin/python
# -*- coding: UTF-8 -*-

import requests
from fake_useragent import UserAgent
import json
import socket
import pandas as pd
import time
import pymongo
import numpy as np

client = pymongo.MongoClient('localhost', 27017)
# 获得数据库
db = client.ITJUZI

ua = UserAgent()
login_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/json;charset=UTF-8',
    'User-Agent': ua.random,
    'Referer': 'https://www.itjuzi.com/'
}

login_payload = {
    "account": "1365956554@qq.com", "password": "IT1234qwer"
}

data_headers = {
    'Origin': 'https://www.itjuzi.com',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Content-Type': 'application/json;charset=UTF-8',
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': ua.random,
    'Connection': 'keep-alive',
    'Referer': 'https://www.itjuzi.com/company',
}

company_payload = {"pagetotal": 121292, "total": 0, "per_page": 30, "scope": "", "sub_scope": "",
                   "round": "", "location": "", "prov": "", "city": "", "status": "", "sort": "", "selected": ""}

login_url = "https://www.itjuzi.com/api/authorizations"
company_url = "https://www.itjuzi.com/api/companys"
mongodb_collection_company = db.itjuzi_company


def login():
    rex = requests.post(login_url, data=json.dumps(login_payload), headers=login_headers)
    print(rex.cookies)
    token = json.loads(rex.text)['data']['token']
    data_headers['Authorization'] = token
    print("登录成功")


def get_company_data(page):
    company_payload['page'] = page
    # 可能会遇到请求失败 则设置5次重新请求
    retry_times = 5
    while retry_times > 0:
        try:
            response = requests.post(company_url, data=json.dumps(company_payload), headers=data_headers,
                                     timeout=(5, 20))
            cookies = response.cookies.get_dict()
            data_headers['Cookie'] = cookies['acw_tc']
            res = json.loads(response.text)
            if res['code'] == 500:
                print("网站发生系统故障.......")
            if res['code'] == 400:
                print("会员过期.......")
            data = res['data']['data']
            print(len(data))
            time.sleep(np.random.randint(3, 7))
            save_to_mongodb(data)
            break
        except socket.timeout:
            print('下载第{}页 第{}次网页请求超时'.format(page, retry_times))
            retry_times -= 1


def save_to_mongodb(data):
    try:
        df = pd.DataFrame(data)
        table = json.loads(df.T.to_json()).values()
        if mongodb_collection_company.insert_many(table):
            print('存储到mongodb成功')
            # time.sleep(np.random.randint(1, 3))
    except Exception as e:
        print('存储到mongodb失败', e)


def spider_itjuzi(page):
    print('下载第%s页:' % page)
    get_company_data(page)
    print("爬取成功")


def job():
    # p = Pool(cpu_count())
    # for page in range(1, 4045):
    #     p.apply_async(spider_itjuzi, args=(page,))
    # p.close()
    # p.join()
    login()
    for page in range(398, 4045):
        spider_itjuzi(page)
    print('下载完成')


if __name__ == '__main__':
    job()
