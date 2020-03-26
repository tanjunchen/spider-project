#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from lxml import etree
import json
import pandas as pd
import datetime
from kangmeizhongyao.common.AreaCodeDecoder import AreaCodeDecoder
import hashlib
from sqlalchemy import create_engine
import numpy as np
from sqlalchemy.sql import text

engine = create_engine('mysql+pymysql://xx:xxx@xxx:33169/xxx?charset=utf8')
data = {
    'loadType': '3',
    'code': 'CD-013',
    'locale': 'zh_CN',
    'expClass': '6',
    'publish_type': '1',
    'date': datetime.datetime.now().strftime("%Y-%m-%d"),
    'jsoncallback': 'a'
}

header = {
    'Referer': 'http://www.kmzyw.com.cn/vchart/zdjk.shtml',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36'
}


def get_content(content):
    content.encoding = 'utf-8'
    tt = str(content.text)[2:-3]
    return tt


def get_date(date):
    # 日期时间的获取
    date_url = "http://cnkmprice.kmzyw.com.cn/pageIndexForJsonp.action?loadType=4&locale=zh_CN" \
               "&expClass=0&publish_type=1&date={0}&updowntype=0&jsoncallback=a" \
               "&wscckey=7c14a5ac9b3f77fb_1557200787".format(date)
    date_content = get_content(requests.get(date_url, headers=header))
    if date_content is not None:
        if json.loads(date_content)['success'] == 'true':
            return json.loads(date_content)['r_data']
    return None


def get_start_date(day):
    now = datetime.datetime.now()
    date = now + datetime.timedelta(days=-day)
    return date.strftime("%Y-%m-%d")


def get_value(start_date, data_url, name, code):
    data.__setitem__('code', code)
    data.__setitem__('date', start_date)
    dd = get_content(requests.get(data_url, params=data, headers=header, timeout=30))
    if dd is not None:
        if json.loads(dd) is not None and json.loads(dd)['success'] == 'true':
            value = json.loads(dd)['dlValue']
            print("正在抓取", flag, value)
            return [flag, value[0].get('name'), value[0].get('hbValue'), value[0].get('hbFloat')]
    print("抓取失败", flag, name)
    return [flag, name, None, None]


def job():
    # 省份地区的划分
    res = get_content(requests.get("http://www.kmzyw.com.cn/vchart/zdjk.shtml", headers=header))
    content = etree.HTML(res)
    names = content.xpath("//ul[@id='cdSelect']//li//a/text()")
    codes = content.xpath("//ul[@id='cdSelect']//li//a/@code")
    data_url = 'http://cnkmprice.kmzyw.com.cn/pageBwindex.action'
    last_update_time = pd.read_sql("select  end_date from new_high_frequency_item where data_source = 'kmzyw'", engine)
    if not last_update_time.empty:
        dd = last_update_time[0, 0]
        global flag
        days = (datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d"),
                                           "%Y-%m-%d") - datetime.datetime.strptime("2019-05-05", "%Y-%m-%d")).days
        for i in range(1, days):
            start_date = get_start_date(i)
            flag = get_date(start_date)
            print(flag, start_date, i)
            if flag == start_date and \
                    (datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d"),
                                                "%Y-%m-%d") - datetime.datetime.strptime(start_date,
                                                                                         "%Y-%m-%d")).days > 0:
                all_result = []
                for k, v in zip(names, codes):
                    all_result.append(get_value(start_date, data_url, k, v))
                df = pd.DataFrame(all_result)
                df.columns = ['date', 'name', 'value', 'rate']  # 赋予列名
                df.to_csv(datetime.datetime.now().strftime("%Y-%m-%d") + "result_data.csv", encoding='gbk', index=False,
                          mode="a", header=False)
            else:
                continue
    else:
        print("数据没有更新源")


def import_data():
    df = pd.read_csv("2016-05-07-result_data-2019-05-05.csv", encoding='gbk').dropna()
    df = df[df['name'] != '东北']
    rename_dict = {
        "rate": "比率",
        "fvalue": "中药材价格指数"
    }
    df = df.rename(columns=rename_dict)
    area_code_decoder = AreaCodeDecoder()
    df["areacode"] = df["name"].map(area_code_decoder.name_to_areacode)
    # import_data = dff[dff['name'] == '进口']
    df = df[df['name'] != '进口']
    df_melt = df.melt(id_vars=["areacode", "name", "fdate"], value_vars=['中药材价格指数'], var_name='item_short_name',
                      value_name='fvalue')
    df_melt["item_name"] = df_melt["name"] + ":" + df_melt["item_short_name"]
    df_melt["data_source"] = "kmzyw"
    df_melt['freq'] = 'W'
    df_melt['unit'] = '元'
    # item表信息
    dff = df_melt.loc[:,
          ["item_name", "freq", "unit", "areacode", "item_short_name", "data_source"]].drop_duplicates()
    dff["item_id"] = dff["item_name"].map(lambda x: "kmzyw" + str(hashlib.md5(x.encode()).hexdigest()))
    # 数据库表中已经存在的项
    exists_item = pd.read_sql("select item_id from new_high_frequency_item where data_source = 'kmzyw'", engine)
    # 新的item
    new_item = dff.loc[np.logical_not(dff["item_id"].isin(exists_item["item_id"]))]
    new_item["start_date"] = '2016-05-07'
    new_data = df_melt.join(dff.set_index("item_name"), on="item_name", lsuffix="_l").loc[:,
               ["item_id", "fvalue", "fdate"]]

    def tx(conn, new_item_x, new_data_x, dff_x):
        # 更新Item
        new_item_x.to_sql("new_high_frequency_item", conn, index=False, if_exists="append")
        # 更新数据
        new_data_x.to_sql("new_high_frequency_item_source_data", conn, index=False, if_exists="append")
        for item_id in dff_x["item_id"].values:
            conn.execute(text("update new_high_frequency_item set end_date = :date where item_id = :item_id"),
                         date=max(new_data['fdate']), item_id=item_id)

    try:
        engine.transaction(tx, new_item, new_data, dff)
        print("插入数据成功")
    except Exception as e:
        print(e, "插入数据失败")


if __name__ == '__main__':
    job()
    import_data()
