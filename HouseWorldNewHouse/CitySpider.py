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


class CitySpider(object):
    def __init__(self, url):
        self.url = url
        self.__session = self.SessionWrapper()
        self.__href_d = self.__district()

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

    def __district(self):
        res = self.__session.get(self.url)
        res.encoding = "gbk"
        html = etree.HTML(res.text)
        href = html.xpath("//li[@id='quyu_name']/a[position()>1]/@href")
        district_name = html.xpath("//li[@id='quyu_name']/a[position()>1]/text()")
        return dict(zip(district_name, href))

    @staticmethod
    def pages(session, url, result_dict):
        page_d = dict()
        res = session.get(url["url"])
        res.encoding = "gbk"
        html = etree.HTML(res.text)
        page_str = html.xpath("//div[@id='sjina_C01_47']/ul/li/a[@class='last']/@href")
        if page_str:
            page_str_abs = urljoin(url["url"], page_str[0])
            regrp = re.match(r"(?P<prefix>.*?b9)(?P<num>\d+)(?P<suffix>.*)", page_str_abs)
            if regrp:
                page_d = {"prefix": regrp.group("prefix"), "num": regrp.group("num"), "suffix": regrp.group("suffix")}
        else:
            result_dict.update({url["name"]: CitySpider.one_page(session, url["url"])})
            return
        pages_list = []
        for page in range(1, int(page_d["num"])):
            page_url = page_d["prefix"] + str(page) + page_d["suffix"]
            pages_list.extend(CitySpider.one_page(session, page_url))
        result_dict.update({url["name"]: pages_list})

    @staticmethod
    def one_page(session, url):
        result = []
        res = session.get(url)
        res.encoding = "gbk"
        html = etree.HTML(res.text)
        house_price = html.xpath("//div[@class='nl_con clearfix']/div[@class='nhouse_price']")
        for div in house_price:
            price = div.xpath("./span/text()")
            if len(price) == 0 or "价格待定" in price[0]:
                continue
            unit = div.xpath("./em/text()")
            if len(unit) == 0 or not (
                    unit[0] == "元/㎡" or unit[0] == "元/平方米" or unit[0] == "元/㎡起" or unit[0] == "元/平方米起"):
                continue
            result.append([price[0], unit[0]])
        return result

    def run(self):
        if len(self.__href_d) == 0:
            return {}
        pool_size = min(cpu_count() * 4, len(self.__href_d))
        result_dict = Manager().dict()
        pool = Pool(pool_size)
        for name in self.__href_d:
            pool.apply_async(CitySpider.pages,
                             args=(
                                 self.__session,
                                 {"name": name, "url": urljoin(self.url, self.__href_d[name])},
                                 result_dict),
                             error_callback=lambda e: print(e))
        pool.close()
        pool.join()
        return dict(result_dict)


def dict_to_array(dic, prefix, result_list):
    for key in dic:
        if isinstance(dic[key], dict):
            dict_to_array(dic[key], [*prefix, key], result_list)
        else:
            for value in dic[key]:
                result_list.append([*prefix, key, *value])


def job(target=None):
    start = time.time()
    with codecs.open("data/format_new_adjust.json", encoding="utf-8") as file:
        d = json.load(file)
    header = True
    for prov in d:
        if target is not None and prov not in target:
            continue
        for i, k in enumerate(d[prov]):
            prov_result = []
            last = time.time()
            city_dict = CitySpider(d[prov][k]).run()
            dict_to_array(city_dict, [prov, k], prov_result)
            if len(prov_result) != 0:
                df = pd.DataFrame(prov_result)
                df.columns = ["prov", "city", "district", "price", "unit"]
                df.to_csv("data/prov_new_house_result.csv", encoding="gbk", index=False, header=header,
                          mode="w" if header else "a+")
                header = False
                print(str((i + 1) / len(d[prov]) * 100)[0:5], "%", d[prov][k], "耗时", time.time() - last, "秒")
    print("任务总耗时", time.time() - start, "秒")


if __name__ == '__main__':
    job()
