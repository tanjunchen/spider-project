#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from lxml import etree
from multiprocessing import Manager, cpu_count, Pool
import requests
from urllib.parse import urljoin
import pandas as pd
from datetime import datetime
import time

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'
}


class PostSpider(object):
    url = "http://www.yb21.cn"

    def index_page(self, url_queue):
        res = requests.get(self.url, headers=headers)
        res.encoding = "gbk"
        html = etree.HTML(res.text)
        city_href = html.xpath("//a/@href")
        for href in city_href:
            url_queue.put(urljoin(self.url, href))

    def spider(self, url_queue, dumps_queue):
        while True:
            main = {}
            url = url_queue.get()
            res = requests.get(url, headers=headers)
            res.encoding = "gbk"
            html = etree.HTML(res.text)
            code_href = html.xpath("//strong/a/@href")
            text = html.xpath("//strong/a/text()")
            if code_href:
                for href in code_href:
                    url_queue.put(urljoin(self.url, href))
            else:
                post_code = html.xpath("//h1/text()")
                content = html.xpath('//td[not(@colspan)][@class="line2"]/text()')
                provice = html.xpath('//td[@width and @bgcolor][last()]/text()')
                city_distric = html.xpath("//td[@width and @bgcolor][last()]/a/text()")
                content = [i.split() for i in content]
                for provice in provice[0].split():
                    provice_city_distric = provice + '-' + city_distric[0] + '-' + city_distric[1]
                    main[provice_city_distric] = {post_code[0]: content}
                    print(main)
                    dumps_queue.put(main)
                    self.dump(dumps_queue)

    def dump(self, dumps_queue):
        main = dumps_queue.get()
        with open('post_code.json', 'a') as f:
            json.dump(main, f, ensure_ascii=False)


def job():
    spider = PostSpider()
    url_queue = Manager().Queue()
    spider.index_page(url_queue)
    while True:
        print(url_queue.get())
    dumps_queue = Manager().Queue()
    spider.index_page(url_queue)
    p = Pool(cpu_count() * 4)
    for i in range(len(url_queue)):
        p.apply_async(spider.spider, args=(url_queue, dumps_queue))
    p.close()
    p.join()


def get_url_by_pandas(url, result_list):
    res = requests.get(url, headers=headers)
    res.encoding = "gbk"
    html = etree.HTML(res.text)
    code_href = html.xpath("//strong/a/@href")
    for n in code_href:
        result_list.append(urljoin(url, n))
    print("Crawling --> ", url)


def spider_url():
    url = "http://www.yb21.cn"
    res = requests.get(url, headers=headers)
    res.encoding = "gbk"
    html = etree.HTML(res.text)
    city_href = html.xpath("//a/@href")

    result_list = Manager().list()
    urls = [urljoin(url, n) for n in city_href]
    p = Pool(cpu_count() * 4)
    for i in urls:
        p.apply_async(get_url_by_pandas, args=(i, result_list))
    p.close()
    p.join()
    with open("YB21S/url.txt", encoding="utf-8", mode='w+') as f:
        for k, v in enumerate(list(result_list)):
            f.write(v + "\n")


def get_address_post_code(u, result_list):
    data_list = []
    print("crawling", u)
    res = requests.get(u, headers=headers)
    if res.status_code == 200:
        res.encoding = "gbk"
        table = pd.read_html(res.text)[2]
        data = table.loc[0:2].iloc[:, [0, 1]]
        post_code = str(data.loc[0][0])
        name = str(data.loc[1][1])
        address = [str(name).replace("-", ",") + "," + n.replace("\xa0", "").replace("邮编", "").replace("邮政编码", "")
                   for n in str(data.loc[2][0]).split(" ")[0:-6]]
        for a in address:
            data_list.append([post_code + ";" + a])
        print(data_list)
        time.sleep(3)
        df = pd.DataFrame(data_list)
        result_list.append(df)
    else:
        print('This is a \033[1;35m 爬太快了 \033[0m!')
        print('This is a \033[1;35m 爬太快了 \033[0m!')
        print('This is a \033[1;35m 爬太快了 \033[0m!')


def spider():
    start = time.time()
    with open("YB21S/url.txt", encoding="utf-8", mode='r') as f:
        data = f.readlines()
    urls = [n.replace("\r", "").replace("\n", "") for n in data]
    p = Pool(cpu_count() * 2)
    result_list = Manager().list()
    for u in urls:
        p.apply_async(get_address_post_code, args=(u, result_list))
    p.close()
    p.join()
    dff = pd.concat(result_list)
    dff.columns = ["邮编;省,市,区,乡/镇"]
    dff.to_csv("YB21S/all_result_" + datetime.now().strftime('%Y-%m-%d') + ".csv", index=False, encoding="utf-8")
    print("耗时", time.time() - start, "秒")


def test():
    # url = "http://www.yb21.cn/post/code/621018.html"
    # result_list = Manager().list()
    # get_address_post_code(url, result_list)
    # df = pd.concat(result_list)
    pass


if __name__ == "__main__":
    # job()
    # spider()
    spider_url()
    # test()
