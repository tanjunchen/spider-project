#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from ChinaHousePrice.common import AreaCodeDecoder
import pymysql
import hashlib
from datetime import datetime
import numpy as np
from sqlalchemy import create_engine


def item_name(address, name):
    address = address.replace(" ", "").replace(",", ":")
    return address + ":" + name if address[-1] != ":" else address[:-1] + ":" + name


def get_conn():
    conn = pymysql.connect(host='url', port=3306, user='root', passwd='xxxx', db='db',
                           charset='utf8')
    return conn


def analysis():
    df = pd.read_csv("des_data/all_cre_result_" + datetime.now().strftime('%Y-%m-%d') + ".csv", encoding='gbk')
    df = df.drop("updateTime", axis=1)
    df = df.rename(columns={"住宅:二手房:价格": "二手房价格:住宅", "住宅:新楼盘:价格": "新楼盘价格:住宅", "住宅:出租:价格": "租金:住宅",
                            "商铺:二手房:价格": "二手房价格:商铺", "商铺:出租:价格": "租金:商铺", "办公:二手房:价格": "二手房价格:办公",
                            "办公:出租:价格": "租金:办公"})
    index_names = ['省', '市', '区', '二手房价格:住宅', '新楼盘价格:住宅', '租金:住宅', '二手房价格:商铺', '租金:商铺', '二手房价格:办公', '租金:办公']
    # df_all
    df_all = df.loc[:, index_names]
    df_all = pd.melt(df_all, id_vars=['省', '市', '区'], var_name='index_name', value_name='value')
    df_all['区'] = df_all['区'].fillna(" ")
    df_all['地址'] = df_all.loc[:, '省'] + "," + df_all.loc[:, '市'] + "," + df_all.loc[:, '区']
    a = AreaCodeDecoder()
    df_all['areacode'] = df_all['地址'].apply(lambda address: a.format_addr_code(address))
    df_all['item_name'] = df_all.apply(lambda df_all: item_name(df_all['地址'], df_all['index_name']), axis=1)
    df_all["item_id"] = df_all['item_name'].map(lambda address: "c" + str(hashlib.md5(address.encode()).hexdigest()))
    df_all['start_date'] = datetime.now().strftime('%Y-%m-%d')
    dff_item = pd.DataFrame()
    dff_data = pd.DataFrame()
    # dff
    dff_item['item_short_name'] = df_all['index_name']
    dff_item["item_id"] = df_all["item_id"]
    dff_item['start_date'] = df_all['start_date']
    dff_item['item_name'] = df_all['item_name']
    dff_item["data_source"] = "HousePrice"
    dff_item["freq"] = "W"
    dff_item['areacode'] = df_all['areacode']
    dff_item = dff_item.dropna()
    # df_data
    dff_data["item_id"] = df_all["item_id"]
    dff_data['fvalue'] = df_all['value']
    dff_data['fdate'] = df_all['start_date']
    dff_data = dff_data.dropna()
    engine = create_engine('mysql+pymysql://root:x@url/db?charset=utf8')
    insert_database(dff_item, dff_data, engine)


def insert_database(df_item: pd.DataFrame, dff_data: pd.DataFrame, engine):
    exists_item = pd.read_sql("select item_id from houseprice_item", engine)  # 查询数据库中的item
    new_item_df = df_item[np.logical_not(df_item["item_id"].isin(exists_item["item_id"]))]  # 查找出新获取的item与数据库中的item的差集
    try:
        new_item_df.to_sql("houseprice_item", engine, index=False, if_exists="append")  # 往数据库中插入新增加的item
        print("xxx")
        dff_data.to_sql("houseprice_data", engine, index=False, if_exists="append")  # 往数据库中插入新获取的数据
        print("xxx")
        # 更新数据库中的item的end_date时间
        con = get_conn()
        cur = con.cursor()
        params = dff_data[['fdate', 'item_id']].values.tolist()
        sql = 'update houseprice_item set end_date = %s where item_id = %s'
        results = cur.executemany(sql, params)
        con.commit()
        print(results)
    except Exception as e:
        print(e)
        print("xxx")
        con.rollback()


if __name__ == '__main__':
    analysis()
