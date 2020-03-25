#!/usr/bin/env python
# -*- coding: utf-8 -*-
from fake_useragent import UserAgent
import codecs
import json
import requests
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
from common import area_code_decoder as acd

ua = UserAgent()


# 随机User-Agent
def headers():
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "maoyan.com",
        "Upgrade-Insecure-Requests": "1",
        'User-Agent': ua.random
    }


# 猫眼地理位置信息
def get_id_name():
    data = {}
    with codecs.open("cities.json", 'r', 'utf-8') as f:
        urls = json.load(f)
    for k, v in urls['letterMap'].items():
        idd = []
        name = []
        for i in range(len(v)):
            idd.append(v[i]['id'])
            name.append(v[i]['nm'])
        data.update(zip(idd, name))
    return data


def get_real_data_list(k, v, url):
    res = requests.get(url, headers=headers(), timeout=30)
    json_data = res.json()["data"]
    if json_data:
        if len(json_data["list"]) > 0:
            box_total_price = res.json()["data"]["splitTotalBoxInfo"] + res.json()["data"]["splitTotalBoxUnitInfo"]
            df = pd.DataFrame(res.json()["data"]["list"])
            data = []
            for value in df['viewInfo'].values:
                data.append(value)
            return [v, k, box_total_price, data]
        else:
            return [v, k, '', '']
    else:
        return [v, k, '', '']


# 从猫眼API获取数据
def get_data(start_date):
    data = get_id_name()
    result_data = []
    for k, v in data.items():
        url = "https://box.maoyan.com/api/cinema/minute/box.json?cityId=" + str(
            k) + "&cityTier=0&beginDate=" + start_date + "&type=0&isSplit=false&utm_term=5.2.3&utm_source=MoviePro_aliyun&utm_medium=android&utm_content=008796762865218&movieBundleVersion=5230&utm_campaign=AmovieproBmovieproCD-1&pushToken=dpshe96a2e871a79ed0abfa261a3be77158batpu&uuid=9B6E409786B2A3848458A63AF3E8AD39030BC38E1E7BEBBEAA88336F8C474C39&deviceId=008796762865218&language=zh"
        _data = get_real_data_list(k, v, url)
        print('Crawling ', start_date, _data)
        result_data.append(_data)
    df = pd.DataFrame(result_data)
    print("抓取猫眼", start_date, "数据成功")
    df = df.replace("", np.nan)
    df.columns = ['name', 'id', 'totalPrice', 'watches']  # 赋予列名
    df = df.dropna()  # 去除空值
    values = []
    for k, v in df['data'].astype('str').iteritems():
        data_value = v.replace('[', '').replace(']', '').replace("'", "").split(',')
        sum = 0
        for i in range(len(data_value)):
            if '万' in data_value[i]:
                data_value[i] = float(data_value[i].replace('万', '')) * 10000
            if data_value[i] == 'nan':
                sum = 0
            else:
                sum = sum + int(data_value[i])
        values.append(sum)
    df['watches'] = values
    df['totalPrice'] = df['totalPrice'].str.replace("万", "").astype(float) * 10000
    df = df[df['totalPrice'] > 0.0]  # 删除总票房小于0.0的值 观影人次少于0的值
    df = df[['watches'] > 0]
    df.to_csv("sourcedata/" + start_date + ".csv", index=False, encoding="gbk")
    df = df[['name', 'totalPrice', 'watches']].copy()
    print("备份csv文档成功")
    analysis(df, start_date)


def analysis():
    df = pd.read_csv("maoyan/sourcedata/xxx.csv", encoding="gbk")
    df['areacode'] = df['name'].apply(
        lambda x: acd.sentence_to_areacode_with_fuzzy(x))
    print(df)


# 爬取主程序
def spider(start_date, end):
    while start_date <= end:
        get_data(start_date)
        start_date = (datetime.strptime(start_date, '%Y%m%d') + timedelta(days=1)).strftime("%Y%m%d")


def get_str_pre_now():
    now = datetime.now().strftime('%Y%m%d')
    return (datetime.strptime(now, '%Y%m%d') + timedelta(days=-1)).strftime('%Y%m%d')


def job():
    # 开始抓取数据
    pre_now = get_str_pre_now()
    spider(pre_now, pre_now)


if __name__ == '__main__':
    job()
    analysis()
