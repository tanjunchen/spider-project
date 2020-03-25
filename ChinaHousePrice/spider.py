#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ChinaHousePrice.session import SessionWrapper
from lxml import etree
import numpy as np
import time
import json
from multiprocessing import Pool, Manager, cpu_count
from urllib.parse import urljoin
import pandas as pd


class Spider(object):
    def __init__(self, prov, city, url):
        self.prov = prov
        self.city = city
        self.url = url
        self.__session = SessionWrapper(timeout=30)
        self.__href_d = self._district()
        self.__city_data = self._city_data()

    def _city_data(self):
        return self.district_all_data(np.nan, self.url)

    def _district(self):
        href_d = {}
        res = self.__session.get(self.url)
        if res is None:
            return href_d
        html = etree.HTML(res.text)
        # 城市 href 名称
        href_n = html.xpath("//span[@class='city-n']/a/@href")
        district_name_n = html.xpath("//span[@class='city-n']/a/span/text()")
        href_d.update(dict(zip(district_name_n, href_n)))
        # 城市 县 href 名称
        href_w = html.xpath("//span[@class='city-w']/a/@href")
        district_name_w = html.xpath("//span[@class='city-w']/a/span/text()")
        href_d.update(dict(zip(district_name_w, href_w)))
        print(self.url + "    parse   ", href_d)
        return href_d

    def _parse_by_xpath(self, url):
        try:
            response = self.__session.get(url)
            if response and response.ok:
                html = etree.HTML(response.text)
                ul = html.xpath("//*[@id='content']/div[4]/div[1]/div[2]/div//div/ul")
            if len(ul) > 1:
                data = (ul[1].xpath("./li/span/text()"))
                if len(data) != 2:
                    raise ValueError
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

    def district_all_data(self, district, url):
        params = ["", "?&type=newha", "?&type=lease", "?&type=lease&proptype=22", "?&proptype=22", "?&proptype=21",
                  "?&type=lease&proptype=22"]
        result = [self.prov, self.city, district]
        for u in params:
            url_ = url + u
            result += self._parse_by_xpath(url_)
            print("Crawl-->" + url_, self._parse_by_xpath(url_))
        return result

    def run(self):
        if len(self.__href_d) == 0:
            return []
        pool_size = min(cpu_count() * 4, len(self.__href_d))
        result_list = Manager().list()
        pool = Pool(pool_size)
        for district in self.__href_d:
            url = urljoin(self.url, self.__href_d[district])
            pool.apply_async(thread, args=(district, url, result_list),
                             error_callback=lambda e: println(e))
        pool.close()
        pool.join()
        result = self.__city_data
        result_list.append(result)
        df = pd.DataFrame(result_list)
        df.columns = ["省", "市", "区", "住宅:二手房:价格", "住宅:二手房:环比", '住宅+新楼盘+价格', '住宅+新楼盘+环比', "住宅:出租:价格", "住宅:出租:环比",
                      "商铺:二手房:价格", "商铺:二手房:环比", "商铺:出租:价格", "商铺:出租:环比",
                      "办公:二手房:价格", "办公:二手房:环比", "办公:出租:价格", "办公:出租:环比"]
        return result_list


def thread(district, url, result_list):
    result_list.append(Spider.district_all_data(district, url))
    return result_list


def job(target=None):
    start = time.time()
    with open("url/format_cre_adjust2.json", encoding="utf-8") as f:
        d = json.load(f)
    for i, prov in enumerate(d):
        last = time.time()
        if target is not None and prov not in target:
            continue
        for city in d[prov]:
            flag = True
            city_data = Spider(prov, city, d[prov][city]).run()
            print(city_data)
        # save_to_csv(result_list,flag)
        print(str((i + 1) / len(d) * 100)[0:5], "%", prov, "Spending", time.time() - last, "Second")
        time.sleep(6)
    print("The Time of Spending ", time.time() - start, "Second")


if __name__ == '__main__':
    job()
