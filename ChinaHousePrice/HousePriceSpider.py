#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import time
from multiprocessing import Pool, Manager, cpu_count
from urllib.parse import urljoin
import numpy as np
import pandas as pd
import hashlib
from lxml import etree
from ChinaHousePrice.session import SessionWrapper
from datetime import datetime
from ChinaHousePrice.common import AreaCodeDecoder
import pymysql
from sqlalchemy import create_engine
from ChinaHousePrice import config
from sqlalchemy.sql import text


class Spider(object):
    def __init__(self, prov, city, url):
        self.prov = prov
        self.city = city
        self.url = url
        self.__session = SessionWrapper(timeout=20)
        self.__href_d = self._district()
        self.city_data = self._city_data()

    def _district(self):
        href_d = {}
        res = self.__session.get(self.url)
        if res is None:
            return href_d
        html = etree.HTML(res.text)
        href_n = html.xpath("//span[@class='city-n']/a/@href")
        district_name_n = html.xpath("//span[@class='city-n']/a/span/text()")
        href_d.update(dict(zip(district_name_n, href_n)))
        href_w = html.xpath("//span[@class='city-w']/a/@href")
        district_name_w = html.xpath("//span[@class='city-w']/a/span/text()")
        href_d.update(dict(zip(district_name_w, href_w)))
        return href_d

    def _city_data(self):
        return self._district_all_data(np.nan, self.url + "?")

    def _district_all_data(self, district, url):
        params = ["", "?&type=newha", "?&type=lease", "?&type=lease&proptype=22", "?&proptype=22", "?&proptype=21",
                  "?&type=lease&proptype=22"]
        result = [self.prov, self.city, district]
        for u in params:
            url_ = url + u
            result += self._parse_by_xpath(url_)
            print("Crawl-->" + url_, self._parse_by_xpath(url_))
        # prov,city,district,住宅+二手房+价格,住宅+二手房+环比,住宅+新楼盘+价格,住宅+新楼盘+环比,住宅+出租+价格,住宅+出租+环比,
        # 商铺+二手房+价格,商铺+二手房+环比,商铺+出租+价格,商铺+出租+环比,
        # 办公+二手房+价格,办公+二手房+环比,办公+出租+价格,办公+出租+环比
        return result

    def _parse_by_xpath(self, url):
        try:
            response = self.__session.get(url)
            if response and response.ok:
                html = etree.HTML(response.text)
                auto = html.xpath("//span[@id='viewkey_wp']")
                if len(auto) > 0:
                    print("\033[31m弹出验证码了......赶紧去解决它.....\033[0m")
                ul = html.xpath("//*[@id='content']/div[4]/div[1]/div[2]/div//div/ul")
            if len(ul) > 1:
                data = (ul[1].xpath("./li/span/text()"))
                # print(data)
                if len(data) != 2:
                    raise ValueError
                if len(data) == 2:
                    data[0] = data[0].replace(",", "")
                    data[1] = data[1].replace(",", "")
                return data
            elif len(ul) == 1:
                data = (ul[0].xpath("./li/span/text()"))
                if len(data) == 1:
                    if data[0] == '--':
                        data[0] = np.nan
                    data.append(np.nan)
                if len(data) != 2:
                    raise ValueError
                return data
            raise ValueError
        except ValueError:
            return [np.nan, np.nan]
        except Exception:
            SessionWrapper.to_exception(url)
            return [np.nan, np.nan]

    def run(self):
        result = [self.city_data]
        for n, h in self.__href_d.items():
            url = urljoin(self.url, h)
            result.append(self._district_all_data(n, url))
        df = pd.DataFrame(result)
        df.columns = ["省", "市", "区", "住宅:二手房:价格", "住宅:二手房:环比", '住宅:新楼盘:价格', '住宅:新楼盘:环比', "住宅:出租:价格", "住宅:出租:环比",
                      "商铺:二手房:价格", "商铺:二手房:环比", "商铺:出租:价格", "商铺:出租:环比",
                      "办公:二手房:价格", "办公:二手房:环比", "办公:出租:价格", "办公:出租:环比"]
        return df


def thread(_prov, _city, _url, _list):
    s = Spider(_prov, _city, _url)
    _list.append(s.run())


def job(target=None):
    start = time.time()
    with open("url/format_cre_adjust.json", encoding="utf-8") as file:
        d = json.load(file)
    header = True
    for i, prov in enumerate(d):
        last = time.time()
        if target is not None and prov not in target:
            continue
        pool_size = min(cpu_count() * 4, len(d[prov]))
        result_list = Manager().list()
        pool = Pool(pool_size)
        for city in d[prov]:
            pool.apply_async(thread,
                             args=(prov, city, d[prov][city], result_list),
                             error_callback=lambda e: print(e))
        pool.close()
        pool.join()
        df = pd.concat(result_list)
        df.to_csv("des_data/all_cre_result_" + datetime.now().strftime('%Y-%m-%d') + ".csv",
                  mode="w" if header else "a+", header=header, index=False,
                  encoding="gbk")
        header = False
        print(str((i + 1) / len(d) * 100)[0:5], "%", prov, "耗时", time.time() - last, "秒")
        time.sleep(5)
    print("任务总耗时", time.time() - start, "秒")


'''数据分析 导入数据到数据库'''


def item_name(address, name):
    address = address.replace(" ", "").replace(",", ":")
    return address + ":" + name if address[-1] != ":" else address[:-1] + ":" + name


def get_conn():
    conn = pymysql.connect(host=config.host, port=config.port, user=config.user, passwd=config.passwd,
                           db=config.db, charset='utf8')
    return conn


def analysis():
    now_x = datetime.now().strftime('%Y-%m-%d')
    df = pd.read_csv("des_data/all_cre_result_" + now_x + ".csv", encoding='gbk')
    # df = pd.read_csv("des_data/all_cre_result_2018-11-30.csv", encoding='gbk')
    df = df.rename(columns={"住宅:二手房:价格": "二手房价格:住宅", "住宅:新楼盘:价格": "新楼盘价格:住宅", "住宅:出租:价格": "租金:住宅",
                            "商铺:二手房:价格": "二手房价格:商铺", "商铺:出租:价格": "租金:商铺", "办公:二手房:价格": "二手房价格:办公",
                            "办公:出租:价格": "租金:办公"})
    index_names = ['省', '市', '区', '二手房价格:住宅', '新楼盘价格:住宅', '租金:住宅', '二手房价格:商铺', '租金:商铺', '二手房价格:办公', '租金:办公']
    # df_all所有的数据
    df_all = df.loc[:, index_names]
    df_all = pd.melt(df_all, id_vars=['省', '市', '区'], var_name='index_name', value_name='value')
    df_all['区'] = df_all['区'].fillna(" ")
    df_all['地址'] = df_all.loc[:, '省'] + "," + df_all.loc[:, '市'] + "," + df_all.loc[:, '区']
    a = AreaCodeDecoder()
    df_all['areacode'] = df_all['地址'].apply(lambda address: a.format_addr_code(address))
    df_all['item_name'] = df_all.apply(lambda df_all: item_name(df_all['地址'], df_all['index_name']), axis=1)
    df_all["item_id"] = df_all['item_name'].map(lambda address: "c" + str(hashlib.md5(address.encode()).hexdigest()))
    df_all["freq"] = "W"
    df_all["data_source"] = "HousePrice"
    rename_dict = {
        "index_name": "item_short_name",
        "value": "fvalue",
    }
    df_all = df_all.rename(columns=rename_dict)
    df_all['unit'] = df_all["item_short_name"].apply(
        lambda x: '元/平方米' if x in ['二手房价格:住宅', '新楼盘价格:住宅', '二手房价格:商铺', '二手房价格:办公'] else '元/月/平方米')
    # 房价网的item
    df_all_item = df_all.loc[:, ["item_id", "item_name", "freq", "unit", "areacode", "item_short_name",
                                 "data_source"]].drop_duplicates()

    # 房价网的数据
    df_all_data = df_all.loc[:, ["item_id", "fvalue"]].dropna().drop_duplicates()
    df_all_item = df_all_item.loc[(df_all_item["item_id"].isin(df_all_data["item_id"]))]
    df_all_data['fvalue'] = df_all_data['fvalue'].apply(lambda x: str(x).replace(',', '')).astype('float64')
    df_all_data['fdate'] = now_x

    engine = create_engine(config.conn_tools)
    exists_item = pd.read_sql("select item_id from " + config.table_item_name + " where data_source = 'HousePrice'",
                              engine)
    # 新的item
    new_item = df_all_item.loc[np.logical_not(df_all_item["item_id"].isin(exists_item["item_id"]))]
    new_item["start_date"] = now_x

    def tx(conn, new_item_x, df_all_data_x, df_all_item_x, now_xx):
        # 更新Item
        new_item_x.to_sql(config.table_item_name, conn, index=False, if_exists="append")
        # 更新数据
        df_all_data_x.to_sql(config.table_item_data, conn, index=False, if_exists="append")
        for item_id in df_all_item_x["item_id"].values:
            conn.execute(text("update " + config.table_item_name + " set end_date = :date where item_id = :item_id"),
                         date=now_xx, item_id=item_id)

    try:
        engine.transaction(tx, new_item, df_all_data, df_all_item, now_x)
        print("插入数据成功")
    except Exception as e:
        print(e)
        print("插入数据失败")


if __name__ == '__main__':
    job()  # 开始房价网的数据的抓取
    analysis()  # 数据分析与数据插入到数据库中
