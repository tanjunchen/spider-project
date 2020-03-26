#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd
from sqlalchemy import create_engine
from ZipCode.common import AreaCodeDecoder
import time
from sqlalchemy.types import NVARCHAR, Float, Integer


def import_data_frame_to_database():
    start_time = time.time()
    dff = pd.read_csv("postcode.csv", encoding='utf-8')
    dff.columns = ['index', 'post_number', 'pro', 'city', 'dis', 'address', 'pro_city_dis']
    conn = create_engine('mysql+pymysql://root:a@xxx:3306/xxx?charset=utf8', encoding='utf8')
    dff.to_sql('postcode', conn, index=False, if_exists="append")
    print("导入数据成功共花费", time.time() - start_time, "秒")


def get_data_from_database_analysis():
    start_time = time.time()
    print("正在加载数据源.....")
    sql = "select * from POST"
    conn = create_engine('mysql+pymysql://root:xxx@host:3306/xxx?charset=utf8', encoding='utf8')
    dff = pd.read_sql(sql, conn)
    print("数据源加载完成,耗时", time.time() - start_time, "秒")
    a = AreaCodeDecoder()
    dff.columns = ['index', 'post_number', 'pro', 'city', 'dis', 'address', 'pro_city_dis']
    dff_two = dff[['post_number', 'pro', 'city', 'dis', 'address', 'pro_city_dis']]
    start_two_time = time.time()
    dff_two['area_code'] = dff_two['pro_city_dis'].apply(lambda address: a.sentence_to_areacode(address)[2])
    print("数据源地址解析完成,各种地址的area_code解析成功,耗时", time.time() - start_two_time, "秒")
    dff_two = dff_two.dropna(subset=['area_code'])
    print("正在去除没有解析成功的数据......")
    print("正在往数据库中导入数据......")
    start_three_time = time.time()
    # dff_two.to_sql('post_to_code', conn, index=False, if_exists="append")
    con = create_engine('mysql+pymysql://root:a@xxx:3306/xxx?charset=utf8', encoding='utf8')
    type_dict = mapping_df_types(dff_two)
    dff_two.to_sql(name='post_to_code', con=con, if_exists='append', index=False, dtype=type_dict)
    print("导入数据成功,耗时", time.time() - start_three_time, "秒")
    print("总共耗时", time.time() - start_time)
    # dff_two.to_csv("result2.csv", encoding='utf-8', index=False)
    # print("生成数据成功......")


def mapping_df_types(df):
    type_dict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            type_dict.update({i: NVARCHAR(length=255)})
        if "float" in str(j):
            type_dict.update({i: Float(precision=2, asdecimal=True)})
        if "int" in str(j):
            type_dict.update({i: Integer()})
    return type_dict


def import_data():
    start_time = time.time()
    dff = pd.read_csv("result.csv", encoding='utf-8')
    dff.columns = ['post_number', 'pro', 'city', 'dis', 'address', 'pro_city_dis', 'area_code']
    dff = dff.dropna(subset=['area_code'])
    dff['area_code'] = dff['area_code'].astype('int64')
    conn = create_engine('mysql+pymysql://root:xxx@host:3306/xxx?charset=utf8', encoding='utf8')
    dff.to_sql('post_to_code', conn, index=False, if_exists="append")
    print("导入数据成功共花费", time.time() - start_time, "秒")


def to_json():
    df = pd.read_csv("result.csv", encoding='utf-8')
    df_two = df[['post_number', 'area_code', 'pro_city_dis']]
    df_two = df_two.dropna(subset=['area_code'])
    df_two['area_code'] = df_two['area_code'].astype('int64')
    df = df_two.groupby(by=['area_code', 'pro_city_dis'])


'''
 删除含有空数据的全部行
df4 = pd.read_csv('4.csv',  encoding='utf-8')
df4 = df4.dropna()

# 可以通过axis参数来删除含有空数据的全部列
df4 = df4.dropna(axis=1)

# 可以通过subset参数来删除在age和sex中含有空数据的全部行
df4 = df4.dropna(subset=["age", "sex"])
print(df4)
df4 = df4.dropna(subset=['age', 'body']) 
'''


def job():
    # import_data_frame_to_database()
    get_data_from_database_analysis()
    # to_json()
    # import_data()


if __name__ == '__main__':
    job()
