#!/usr/bin/python
# -*- coding: UTF-8 -*-

import requests
from lxml import etree
import re
import pandas as pd
import arrow
import datetime
import os
from common.AreaCodeDecoder import AreaCodeDecoder
import hashlib
from sqlalchemy import create_engine
import numpy as np
from sqlalchemy.sql import text

engine = create_engine('mysql+pymysql://xxx:xxx@host:port/xxx?charset=utf8')


def get_address():
    res = requests.get("https://www.daojia.com/selectCity/"
                       "?category=yuesao&backCityUrl=https://www.daojia.com/jiage/_city_/yuesao/")
    res.encoding = 'utf-8'
    content = etree.HTML(res.text)
    city_names = content.xpath("//div[@class='search_b']//ul//span//a/text()")
    city_urls = content.xpath("//div[@class='search_b']//ul//span//a/@href")
    dd = zip(city_names, city_urls)
    return dd


def get_data():
    address = get_address()
    data = []
    now = arrow.now()
    now_month = now.datetime.month
    for k, v in address:
        res = requests.get(v)
        res.encoding = 'utf-8'
        months = re.findall(r'categories: (.*?)}', res.text, re.S)[0].strip()
        values = re.findall(r'data: (.*?)\n', res.text, re.S)[0]
        months = months.replace('[', '').replace(']', '').replace('"', '').split(",")
        values = values.replace("[", "").replace("]", "").split(",")
        i = 0
        if str(now_month) == months[len(months) - 1].replace("月", ""):
            while i < len(values):
                day = now.shift(months=-i).strftime("%Y-%m-%d")[:-2] + "01"
                data.append([k, day, values[len(values) - i - 1]])
                i = i + 1
            print("正在抓取", k, v)
    df = pd.DataFrame(data)
    df.columns = ["city", "fdate", "fvalue"]
    df.to_csv(now.strftime("%Y-%m-%d") + "data.csv", index=False, encoding="gbk")


def analysis():
    source_data = datetime.datetime.now().strftime("%Y-%m-%d") + "data.csv"
    if os.path.exists(source_data):
        print("已经爬取了新的数据")
        df = pd.read_csv(source_data, encoding='gbk')
        rename_dict = {
            "fvalue": "月嫂薪资"
        }
        df = df.rename(columns=rename_dict)
        area_code_decoder = AreaCodeDecoder()
        df["areacode"] = df["city"].map(area_code_decoder.name_to_areacode)
        df = df.dropna()
        df_melt = df.melt(id_vars=["areacode", "city", "fdate"], value_vars=['月嫂薪资'], var_name='item_short_name',
                          value_name='fvalue')
        df_melt["item_name"] = df_melt["city"] + ":" + df_melt["item_short_name"]
        df_melt["data_source"] = "58daojia"
        df_melt['freq'] = 'M'
        df_melt['unit'] = '元'

        dff = df_melt.loc[:,
              ["item_name", "freq", "unit", "areacode", "item_short_name", "data_source"]].drop_duplicates()
        dff["item_id"] = dff["item_name"].map(lambda x: "58daojia" + str(hashlib.md5(x.encode()).hexdigest()))
        # 数据库表中已经存在的项
        exists_item = pd.read_sql("select item_id from new_high_frequency_item where data_source = '58daojia'", engine)
        # 新的item
        new_item = dff.loc[np.logical_not(dff["item_id"].isin(exists_item["item_id"]))]
        new_item["start_date"] = '2018-12-01'
        new_data = df_melt.join(dff.set_index("item_name"), on="item_name", lsuffix="_l").loc[:,
                   ["item_id", "fvalue", "fdate"]]

        def tx(conn, new_item_x, new_data_x, dff_x):
            # 更新Item
            new_item_x.to_sql("new_high_frequency_item", conn, index=False, if_exists="append")
            # 更新数据
            new_data_x.to_sql("new_high_frequency_item_source_data", conn, index=False, if_exists="append")
            for item_id in dff_x["item_id"].values:
                conn.execute(text("update new_high_frequency_item set end_date = :date where item_id = :item_id"),
                             date=datetime.datetime.now().strftime("%Y-%m-%d"), item_id=item_id)

        try:
            engine.transaction(tx, new_item, new_data, dff)
            print("插入数据成功")
        except Exception as e:
            print(e, "插入数据失败")
    else:
        print("暂无新的数据")


def beijing():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    source_data = today + "data.csv"
    if os.path.exists(source_data):
        df = pd.read_csv(source_data, encoding='gbk')
        df = df[df['city'] == '北京']
        df['item_id'] = '58daojiayuesao'
        dff = df[['item_id', 'fdate', 'fvalue']]
        data = pd.read_sql(" select * from tb_h5_wind_data where item_id   = "
                           "( select item_id from tb_h5_wind_item where data_source = '58daojia') ", engine)

        def tx(conn, end_date):
            # 更新数据
            dd.to_sql("tb_h5_wind_data", con=engine, index=False, if_exists="append")
            conn.execute(text("update tb_h5_wind_item set end_date = :date"
                              "  where item_id = :item_id  and data_source = '58daojia' "),
                         date=end_date, item_id='58daojiayuesao')

        if max(data['fdate']) < max(dff['fdate']):
            dd = dff[max(dff['fdate'])]
            try:
                engine.transaction(tx, max(dff['fdate']))
                print("插入数据成功")
            except Exception as e:
                print(e, "插入数据失败")
        else:
            print("暂无新数据")


if __name__ == '__main__':
    get_data()
    analysis()
    beijing()
