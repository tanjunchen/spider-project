#!/usr/bin/python
# -*- coding: UTF-8 -*-


'''
http://www.yb21.cn/
'''

import requests
from lxml import etree
from urllib.parse import urljoin


# def job1():
#     res = requests.get('http://www.yb21.cn/')
#     res.encoding = 'gbk'
#     html = etree.HTML(res.text)
#     print(res.text)
#     content = html.xpath("//div[@class='citysearch']")
#     for div in content:
#         pro = div.xpath("//h1/text()")
#         urls = div.xpath("//ul//a/@href")
#         names = div.xpath("//ul//a/strong/text()")
#         print(pro)
#         print(names)
#         print(urls)
#         print("==================")

def job2():
    index = 'http://www.ip138.com/post/'
    res = requests.get(index)
    res.encoding = 'gbk'
    html = etree.HTML(res.text)
    print(res.text)
    name = html.xpath("//div[@id='newAlexa']//tr//td//a/text()")
    href = html.xpath("//div[@id='newAlexa']//tr//td//a/@href")
    hrefs = [urljoin(index, h) for h in href]
    data = zip(name, hrefs)
    # for k, v in data:
    # print(k,v)
    # get_content(k, v)
    # get_content('北京市', 'http://www.ip138.com/10/')
    get_content('陕西省', 'http://www.ip138.com/71/')


def get_content(pro, url):
    res = requests.get(url)
    res.encoding = 'gbk'
    html = etree.HTML(res.text)
    city_names = html.xpath("//table[@class='t12']//tr[@bgcolor='#ffffff']//td/text()")
    city_urls = html.xpath("//table[@class='t12']//tr[@bgcolor='#ffffff']//td/a/text()")
    city_urlss = [i for i in city_urls[::2]]
    print(city_names)
    print(city_urlss)


if __name__ == '__main__':
    job2()
