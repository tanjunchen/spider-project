#!/usr/bin/python
# -*- coding: UTF-8 -*-

import requests
import chardet
import re
from lxml import etree
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin


def run():
    User_Agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'

    headers = {
        'User-Agent': User_Agent
    }
    page_url = "http://baike.baidu.com/view/284853.htm"
    res = requests.get(page_url, headers=headers)
    res.encoding = chardet.detect(res.content)['encoding']
    html = bs(res.text, 'lxml')
    new_urls = set()
    # 抽取符合要求的a标签
    links = html.find_all('a', href=re.compile(r'/item/.*'))
    for link in links:
        # 提取href属性
        new_url = link['href']
        # 拼接成完整网址
        new_full_url = urljoin(page_url, new_url)
        new_urls.add(new_full_url)
    print(new_urls)


if __name__ == '__main__':
    run()
