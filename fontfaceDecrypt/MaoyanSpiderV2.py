#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from lxml import html
import re
import woff2otf
from fontTools.ttLib import TTFont
from bs4 import BeautifulSoup as bs


# 抓取maoyan票房
class MaoyanSpider:
    # 页面初始化
    def __init__(self):
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.8",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36"
        }

    # 获取票房
    def getNote(self):
        url = "http://maoyan.com/cinema/15887?poi=91871213"
        host = {'host': 'maoyan.com',
                'refer': 'http://maoyan.com/news', }
        headers = dict(self.headers.items() + host.items())
        # 获取页面内容
        r = requests.get(url, headers=headers)
        # print r.text
        response = html.fromstring(r.text)
        u = r.text
        # 匹配ttf font
        cmp = re.compile(",\nurl\('(//.*.woff)'\) format\('woff'\)")
        rst = cmp.findall(r.text)
        ttf = requests.get("http:" + rst[0], stream=True)
        with open("maoyan.woff", "wb") as pdf:
            for chunk in ttf.iter_content(chunk_size=1024):
                if chunk:
                    pdf.write(chunk)
        # 转换woff字体为otf字体
        woff2otf.convert('maoyan.woff', 'maoyan.otf')
        # 解析字体库font文件
        baseFont = TTFont('base.otf')
        maoyanFont = TTFont('maoyan.otf')
        uniList = maoyanFont['cmap'].tables[0].ttFont.getGlyphOrder()
        numList = []
        baseNumList = ['.', '3', '5', '1', '2', '7', '0', '6', '9', '8', '4']
        baseUniCode = ['x', 'uniE64B', 'uniE183', 'uniED06', 'uniE1AC', 'uniEA2D', 'uniEBF8',
                       'uniE831', 'uniF654', 'uniF25B', 'uniE3EB']
        for i in range(1, 12):
            maoyanGlyph = maoyanFont['glyf'][uniList[i]]
            for j in range(11):
                baseGlyph = baseFont['glyf'][baseUniCode[j]]
                if maoyanGlyph == baseGlyph:
                    numList.append(baseNumList[j])
                    break
        uniList[1] = 'uni0078'
        utf8List = [eval("u'\\u" + uni[3:] + "'").encode("utf-8") for uni in uniList[1:]]
        # 获取发帖内容
        soup = bs(u, "html.parser")
        index = soup.find_all('div', {'class': 'show-list'})

        print('---------------Prices-----------------')
        for n in range(len(index)):
            mn = soup.find_all('h3', {'class': 'movie-name'})
            ting = soup.find_all('span', {'class': 'hall'})
            mt = soup.find_all('span', {'class': 'begin-time'})
            mw = soup.find_all('span', {'class': 'stonefont'})
            for i in range(len(mt)):
                moviename = mn[i].get_text()
                film_ting = ting[i].get_text()
                movietime = mt[i].get_text()
                moviewish = mw[i].get_text().encode('utf-8')
                for i in range(len(utf8List)):
                    moviewish = moviewish.replace(utf8List[i], numList[i])
                print(moviename, film_ting, movietime, moviewish)


spider = MaoyanSpider()
spider.getNote()
