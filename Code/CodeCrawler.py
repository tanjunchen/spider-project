import codecs
from multiprocessing import Manager
from multiprocessing import Pool
from threading import Thread
from urllib.parse import urljoin
import os
import requests
from lxml import etree

file_name = "stats_area_code.txt"
first_url = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2017/"
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Host": "www.stats.gov.cn",
    "If-Modified-Since": "Thu, 05 Jul 2018 00:43:11 GMT",
    "If-None-Match": "17b5-57035d4e665c0-gzip",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36",
}


def first_page(url, qin, qout):
    res = requests.get(url, headers=headers)
    res.encoding = "gb2312"
    html = etree.HTML(res.text)
    aaa = html.xpath("//a[not(@class)]")
    hrefs = [a.xpath("./@href")[0] for a in aaa]
    codes = [h.replace(".html", "") + "0" * 10 for h in hrefs]
    texts = [a.xpath("./text()")[0] for a in aaa]
    result = list(zip(codes, texts, [urljoin(url, h) for h in hrefs]))
    for r in result:
        qin.put(r)
        qout.put((r[0], r[1]))


def spider(qin, qout):
    while True:
        item = qin.get()
        try:
            res = requests.get(item[2], headers=headers)
            res.encoding = "gb2312"
            html = etree.HTML(res.text)
            aaa = html.xpath("//a[not(@class)]")
            if aaa:
                hrefs = [a.xpath("./@href")[0] for a in aaa[::2]]
                codes = [a.xpath("./text()")[0] for a in aaa[::2]]
                texts = [a.xpath("./text()")[0] for a in aaa[1::2]]
                result = list(zip(codes, texts, [urljoin(item[2], h) for h in hrefs]))
                for r in result:
                    qin.put(r)
                    qout.put([r[0], r[1]])
                codes = html.xpath("//tr[position()>1]/td[1]/text()")
                texts = html.xpath("//tr[position()>1]/td[2]/text()")
                result = list(zip(codes, texts))
                if result:
                    for r in result:
                        qout.put(r)
            else:
                codes = html.xpath("//tr[position()>1]/td[1]/text()")
                texts = html.xpath("//tr[position()>1]/td[3]/text()")
                result = list(zip(codes, texts))
                for r in result:
                    qout.put(r)
        except BaseException as e:
            qin.put(item)
            print(e)


def dump(qout, filename):
    while True:
        item = qout.get()
        try:
            line = ",".join(item)
            with codecs.open(filename, "a", encoding="utf8") as file:
                file.write(line + "\n")
            print(line)
        except BaseException as e:
            qout.put(item)
            print(e)


class ThreadPool(object):
    def __init__(self, size, target, args):
        self.size = size
        self.target = target
        self.args = args
        self.pool = []
        for _ in range(self.size):
            self.pool.append(Thread(target=self.target, args=self.args))

    def start(self):
        for t in self.pool:
            t.start()

    def join(self):
        for t in self.pool:
            t.join()


def run(qin, qout):
    pool_size = 12
    spider_pool = ThreadPool(pool_size, target=spider, args=(qin, qout))
    spider_pool.start()
    spider_pool.join()


def main():
    pool_size = 3
    manager = Manager()
    qin = manager.Queue()
    qout = manager.Queue()
    first_page(first_url, qin, qout)
    pool = Pool(pool_size + 1)
    pool.apply_async(dump, (qout, file_name), error_callback=print)
    for _ in range(pool_size):
        pool.apply_async(run, (qin, qout), error_callback=print)
    pool.close()
    pool.join()


if __name__ == '__main__':

    try:
        os.remove(file_name)
    except Exception as e:
        print(e)
    main()
