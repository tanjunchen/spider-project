#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs
import json
import re
import time
from multiprocessing import Pool, Manager, cpu_count
from urllib.parse import urljoin
import pandas as pd
import requests
from lxml import etree
from bs4 import BeautifulSoup


class CitySpider(object):
    def __init__(self, url, pro):
        self.pro = pro
        self.url = url
        self.__session = self.SessionWrapper()
        self.__href_d = self.__district()

    # SessionWrapper类
    class SessionWrapper(object):
        def __init__(self):
            self.__session = requests.session()

        class EmptyResponse(object):
            pass

        def get(self, url, **kwargs):
            if not kwargs.__contains__("timeout"):
                kwargs["timeout"] = 3
            while True:
                try:
                    res = self.__session.get(url, **kwargs)
                    return res
                except requests.exceptions.ReadTimeout:
                    pass
                except requests.exceptions.ConnectionError:
                    res = self.EmptyResponse()
                    res.text = "<html\>"
                    return res

    # 获取地区的URL
    '''
    以字典的形式返回地区的url
    '''

    def __district(self):
        respnose = self.__session.get(self.url, allow_redirects=False)
        respnose.encoding = 'gbk'
        html = etree.HTML(respnose.text)
        # 获取北京地区标签url
        if self.pro == '北京':
            href = html.xpath("//div[@id='list_D02_10']//a/@href")
            district_name = html.xpath("//div[@id='list_D02_10']//a/text()")
        else:
            href = html.xpath("//li[@id='list_D02_10']//a/@href")
            district_name = html.xpath("//li[@id='list_D02_10']//a/text()")
        print(href + district_name)
        return dict(zip(district_name, href))

    @staticmethod
    def get_pages(session, url, result_dict):
        page_dict = dict()
        res = session.get(url["url"])
        res.encoding = "gbk"
        html = etree.HTML(res.text)
        # 取得末页的地址
        page_num = html.xpath("//div[@id='list_D10_15']/p[last()]/text()")
        if len(page_num) > 0:
            pages = int(re.findall("\d+", page_num[0])[0])
            if pages > 1:
                page_url = url["url"] + "h316-i3"
                if pages > 8:
                    pages = 8
                if page_url:
                    page_dict = {"prefix": page_url, "num": pages}
            elif pages == 1:
                result_dict.update({url["name"]: CitySpider.get_one_page(session, url["url"] + "h316")})
                return
            else:
                return
        else:
            return
        pages_list = []
        if len(page_url) > 0:
            for page in range(1, int(page_dict["num"]) + 1):
                page_url = page_dict["prefix"] + str(page)
                pages_list.extend(CitySpider.get_one_page(session, page_url))
            result_dict.update({url["name"]: pages_list})

    @staticmethod
    def get_one_page(session, url):
        result = []
        res = session.get(url)
        res.encoding = "gbk"
        soup = BeautifulSoup(res.text, 'lxml')
        size_list = soup.select('.clearfix > dd > .tel_shop')  # 房屋面积list
        totalprice_list = soup.select('dd.price_right > span.red > b')  # 总价list
        unitprice_list = soup.select('dd.price_right > span:nth-of-type(2)')  # 单价list
        standard_price = soup.select('.col14')  # 某地区的参考均价 目前此地区房屋出售量
        if standard_price.__len__() > 0:
            avg_price = standard_price[0].text  # 某地区的参考均价
            area_house_sum = standard_price[1].text  # 目前此地区房屋出售量
        else:
            avg_price = ''
            area_house_sum = ''
        size = [s.text.replace("\r\n", "").split("|")[1].replace(" ", "").replace("㎡", "") for s in
                size_list]  # 房屋面积具体值
        totalprice = [t.text.replace("\r\n", "") for t in totalprice_list]  # 总价
        unitprice = [v.text.replace("\r\n", "").replace("元/㎡", "") for v in unitprice_list]  # 单价
        # 主要存储的数据为城市,地区,总价,单价,房屋面积大小,均价,目前房屋销售量 7指标
        for i in range(totalprice.__len__()):
            print([url, totalprice[i], unitprice[i], size[i], str(avg_price), str(area_house_sum)])
            result.append([totalprice[i], unitprice[i], size[i], str(avg_price), str(area_house_sum)])
        return result

    def run(self):
        if len(self.__href_d) == 0:
            return {}
        pool_size = min(cpu_count() * 4, len(self.__href_d))
        result_dict = Manager().dict()
        pool = Pool(pool_size)
        for name in self.__href_d:
            pool.apply_async(CitySpider.get_pages,
                             args=(
                                 self.__session,
                                 {"name": name, "url": urljoin(self.url, self.__href_d[name])},
                                 result_dict),
                             error_callback=lambda e: print(e))
        pool.close()
        pool.join()
        return dict(result_dict)


def data_to_darray(dic, prefix, result_list):
    for key in dic:
        if isinstance(dic[key], dict):
            data_to_darray(dic[key], [*prefix, key], result_list)
        else:
            for value in dic[key]:
                result_list.append([*prefix, key, *value])


def job():
    start_time = time.time()
    # with codecs.open("data/esf2.json",encoding='utf-8') as f:
    with codecs.open("data/format_esf_adjust.json", encoding='utf-8') as f:
        data = json.load(f)
    flag = True
    for province in data:
        for k, v in enumerate(data[province]):
            province_result = []
            last_time = time.time()
            # 城市数据抓取
            city_dict = CitySpider(data[province][v], province).run()
            # 城市数据合并成省份数据
            data_to_darray(city_dict, [province, v], province_result)
            if len(province_result) != 0:
                df = pd.DataFrame(province_result)
                df.columns = ["province", "city", "district", "total_price", "unit_price", "size", "standard_price",
                              "sale_count"]
                df.to_csv("data/province_esf_result.csv", encoding="gbk", index=False, header=flag,
                          mode="w" if flag else "a+")
                flag = False
                print(data[province][v], "耗时", time.time() - last_time, "秒")
    print("任务总耗时", (time.time() - start_time), "秒")


if __name__ == '__main__':
    job()
