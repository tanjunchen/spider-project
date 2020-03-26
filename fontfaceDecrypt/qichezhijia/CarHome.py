#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from lxml import etree
import re
import sys
import io
from fontTools.ttLib import TTFont

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='gb18030')


# 抓取autohome评论
class AutoSpider:
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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
            'host': 'club.autohome.com.cn',
            'cookie': '__ah_uuid=C526DAD3-76F6-42C8-956B-4CBE18611E7B; fvlid=1545293100124hzVSzLWmuB; sessionip=61.149.5.137; area=119999; sessionid=60F897CD-E743-449D-BEBE-44D6533DE992%7C%7C2018-12-20+16%3A05%3A06.291%7C%7Cwww.baidu.com; ahpau=1; sessionuid=60F897CD-E743-449D-BEBE-44D6533DE992%7C%7C2018-12-20+16%3A05%3A06.291%7C%7Cwww.baidu.com; pbcpopclub=0fb65f3c-0c57-43b3-ade4-2752c0517737; ref=www.baidu.com%7C0%7C0%7C0%7C2018-12-20+17%3A56%3A26.073%7C2018-12-20+16%3A05%3A06.291; autoac=DB6482147B98F5F9B9D0834939744526; autotc=3A5FEFF1E14636EA47902DA601BB1DF6; ahpvno=12'}
        # 获取评论

    def getNote(self):
        url = "https://club.autohome.com.cn/bbs/thread-c-2778-69436529-1.html"
        # 获取页面内容
        r = requests.get(url, headers=self.headers)
        html = etree.HTML(r.text)
        # 匹配ttf font
        cmp = re.compile(",url\('(//.*.ttf)'\)")
        rst = cmp.findall(r.text)
        ttf = requests.get("http:" + rst[0], stream=True)
        with open("autohome.ttf", "wb") as pdf:
            for chunk in ttf.iter_content(chunk_size=1024):
                if chunk:
                    pdf.write(chunk)
        # 解析字体库font文件
        font = TTFont('autohome.ttf')
        uniList = font['cmap'].tables[0].ttFont.getGlyphOrder()
        utf8List = [str(uni[3:]) for uni in uniList[1:]]
        wordList = ['一', '七', '三', '上', '下', '不', '中', '档', '比', '油', '泥', '灯',
                    '九', '了', '二', '五', '低', '保', '光', '八', '公', '六', '养', '内', '冷',
                    '副', '加', '动', '十', '电', '的', '皮', '盘', '真', '着', '路', '身', '软',
                    '过', '近', '远', '里', '量', '长', '门', '问', '只', '右', '启', '呢', '味',
                    '和', '响', '四', '地', '坏', '坐', '外', '多', '大', '好', '孩', '实', '小',
                    '少', '短', '矮', '硬', '空', '级', '耗', '雨', '音', '高', '左', '开', '当',
                    '很', '得', '性', '自', '手', '排', '控', '无', '是', '更', '有', '机', '来']
        print(utf8List)
        # 获取发帖内容
        text = html.xpath("string(//div[@class='tz-paragraph'])")
        # note = [ii.replace("\r", "").replace("\n", "") for ii in text]
        # notes = [i.replace("\\u", "") for i in note]
        # print(notes)
        for i in range(len(utf8List)):
            text = text.replace(utf8List[i], wordList[i])
        print(text)


spider = AutoSpider()
spider.getNote()
