#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
# 模拟浏览器
import random
import numpy as np
import codecs
import json
import time
from datetime import datetime, date, timedelta
import requests
from maoyan.common.AreaCodeDecoder import AreaCodeDecoder
from sqlalchemy import create_engine
import pymysql
import hashlib
from maoyan import config
from sqlalchemy.sql import text

USER_AGENTS = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
]


# 随机User-Agent
def header():
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "maoyan.com",
        "Upgrade-Insecure-Requests": "1",
        'User-Agent': random.choice(USER_AGENTS)
    }


# 爬取主程序
def spider(start_date, end):
    while start_date <= end:
        get_data(start_date)
        start_date = (datetime.strptime(start_date, '%Y%m%d') + timedelta(days=1)).strftime("%Y%m%d")


# 全国位置信息
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
    res = requests.get(url, headers=header(), timeout=30)
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


# 从猫眼 API 获取数据
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
    df.columns = ['name', 'id', 'totalPrice', 'data']  # 赋予列名
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
    df['data'] = values
    df['totalPrice'] = df['totalPrice'].str.replace("万", "").astype(float) * 10000
    df = df[df['totalPrice'] > 0.0]  # 删除总票房小于0.0的值 观影人次少于0的值
    df = df[df['data'] > 0]
    import_data_to_maoyan(df, start_date)


# 插入数据到数据库中
def import_data_to_maoyan(df: pd.DataFrame, start_date):
    start_date = datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
    """
     猫眼数据事务入库
     """
    area_code_decoder = AreaCodeDecoder()
    df["areacode"] = df["name"].map(area_code_decoder.name_to_areacode)
    rename_dict = {
        "data": "观影人次(猫眼)",
        "totalPrice": "电影票房(猫眼)"
    }
    df = df.rename(columns=rename_dict)
    df_melt = df.melt(id_vars=["areacode", "name"], value_vars=['观影人次(猫眼)', '电影票房(猫眼)'], var_name='item_short_name',
                      value_name='fvalue')
    df_melt["item_name"] = df_melt["name"] + ":" + df_melt["item_short_name"]
    df_melt["data_source"] = "maoyan"
    df_melt['fdate'] = start_date
    df_melt['freq'] = 'D'
    df_melt['unit'] = df_melt["item_short_name"].apply(lambda x: "元" if x == "电影票房(猫眼)" else "人")
    dff = df_melt.loc[:,
          ["item_name", "freq", "unit", "areacode", "item_short_name", "data_source"]].drop_duplicates()
    dff["item_id"] = dff["item_name"].map(lambda x: "m" + str(hashlib.md5(x.encode()).hexdigest()))

    engine = create_engine(config.conn_tools)
    exists_item = pd.read_sql("select item_id from " + config.table_item_name + " where data_source = 'maoyan'", engine)
    # 新的 item
    new_item = dff.loc[np.logical_not(dff["item_id"].isin(exists_item["item_id"]))]
    new_item["start_date"] = start_date
    # 新的数据
    new_data = df_melt.join(dff.set_index("item_name"), on="item_name", lsuffix="_l").loc[:,
               ["item_id", "fvalue", "fdate"]]

    def tx(conn, new_item_x, new_data_x, dff_x):
        # 更新 Item
        new_item_x.to_sql(config.table_item_name, conn, index=False, if_exists="append")
        # 更新数据
        new_data_x.to_sql(config.table_item_data, conn, index=False, if_exists="append")
        for item_id in dff_x["item_id"].values:
            conn.execute(text("update " + config.table_item_name + " set end_date = :date where item_id = :item_id"),
                         date=start_date, item_id=item_id)

    try:
        engine.transaction(tx, new_item, new_data, dff)
        print("插入数据成功")
    except Exception as e:
        print("插入数据失败")


def get_str_pre_now():
    now = datetime.now().strftime('%Y%m%d')
    return (datetime.strptime(now, '%Y%m%d') + timedelta(days=-1)).strftime('%Y%m%d')


def job():
    # 开始抓取数据
    pre_now = get_str_pre_now()  # 前天的日期
    spider(pre_now, pre_now)
    # 不填任何数值 默认为当前时间 即爬取昨天的数据
    spider(pre_now, pre_now)
    df = pd.read_csv("deal_source_data/xxx.csv", encoding="gbk")
    import_data_to_maoyan(df, 'xxx')


if __name__ == '__main__':
    job()
