#!/usr/bin/python
# -*- coding: UTF-8 -*-

import requests
from fake_useragent import UserAgent
from lxml import etree
import json
from urllib import parse

ua = UserAgent()
headers = {"User-Agent": ua.random,
           "Referer": "http://gz.ziroom.com/"}


def job():
    # content = requests.get("url":"http://hz.ziroom.com/", headers=headers)
    # content.encoding = "utf-8"
    # print(content.text)
    # html = etree.HTML(content.text)
    # print(html.xpath("//dd[@id='cityList']//a/text()"))
    # print(html.xpath("//dd[@id='cityList']//a/@href"))

    [{'name': "北京", 'cityCode': "110000", "url": "http://www.ziroom.com"},
     {'name': "上海", 'cityCode': "310000", "url": "http://sh.ziroom.com"},
     {'name': "深圳", 'cityCode': "440300", "url": "http://sz.ziroom.com"},
     {'name': "杭州", 'cityCode': "330100", "url": "http://hz.ziroom.com"},
     {'name': "南京", 'cityCode': "320100", "url": "http://nj.ziroom.com"},
     {'name': "广州", 'cityCode': "440100", "url": "http://gz.ziroom.com"},
     {'name': "成都", 'cityCode': "510100", "url": "http://cd.ziroom.com"},
     {'name': "武汉", 'cityCode': "420100", "url": "http://wh.ziroom.com"},
     {'name': "天津", 'cityCode': "120000", "url": "http://tj.ziroom.com"}]


def run():
    url = "http://www.ziroom.com/z/nl/z2-d23008614.html?p=48"
    res = requests.get(url, headers=headers)
    res.encoding = "utf-8"
    html = etree.HTML(res.text)
    name = html.xpath("//div[@class='selection_con']/dl[2]/dd/ul/li[position()>1]/span/a/text()")
    urls = html.xpath("//div[@class='selection_con']/dl[2]/dd/ul/li[position()>1]/span/a/@href")
    urls = [parse.urljoin(url, n) for n in urls]
    # data = zip(name, urls)
    print(name, urls)
    page_num = html.xpath("//span[@class='pagenum']/text()")[0].strip("/")
    print(page_num)


if __name__ == '__main__':
    run()
