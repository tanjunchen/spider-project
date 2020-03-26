#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from lxml import etree
import requests
import sys
import io
from fontTools.ttLib import TTFont
from bs4 import BeautifulSoup as bs

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='gb18030')

url_one = "https://maoyan.com/films/1212512"
headers = {
    'authority': 'maoyan.com',
    'method': 'GET',
    'path': '/films/1212512',
    'scheme': 'https',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
}
res = requests.get(url=url_one, headers=headers)
# print(res.text)
soup = bs(res.text, 'lxml')
data = soup.find_all(name='span', attrs={"class": "stonefont"})
print(data)
# html = etree.HTML(res.text)
# stonefont = html.xpath('//span[@class="stonefont"]/text()')
# print(stonefont)


# cmp = re.compile(",\n           url\('(//.*?woff)'\) format\('woff'\)")
# url_2 = 'http:' + cmp.findall(res.text)[0]
# res = requests.get(url=url_2, headers=headers)
# with open('online.woff', 'wb') as f:
#     for i in res.iter_content(chunk_size=1024):
#         if i:
#             f.write(i)
#
# baseFont = TTFont('base.woff')
# onlineFont = TTFont('online.woff')
# dic = {'uniE1DA': '5', 'uniF297': '2', 'uniEFAE': '8', 'uniF00F': '4', 'uniE3BE': '3', 'uniF790': '7', "uniF59E": '6',
#        "uniEFA4": '1', "uniF59F": '0', 'uniF686': '9'}
# uni_list = onlineFont.getGlyphNames()[1:-1]
# print(uni_list)
# temp = {}
# for i in range(10):
#     onlineGlyph = onlineFont['glyf'][uni_list[i]]
#     for j in dic:
#         baseGlyph = baseFont['glyf'][j]
#         if onlineGlyph == baseGlyph:
#             temp["&#x" + uni_list[i][3:].lower() + ';'] = dic[j]
# print(temp)
