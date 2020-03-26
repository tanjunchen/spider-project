#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import pandas as pd


def job():
    url = 'http://www.goubanjia.com/'
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'UM_distinctid=168132a602e1aa-03790ea652ad7b-58422116-1fa400-168132a602f615; JSESSIONID=E0498A8975ACD4E859943603D02E58F4; CNZZDATA1253707717=1042893761-1546504875-null%7C1546581709',
        'Host': 'www.goubanjia.com',
        'Referer': 'http://www.goubanjia.com/',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
    }
    res = requests.get(url=url, headers=headers)
    res.encoding = 'utf-8'
    df = pd.read_html(res.text)[0].loc[:]
    print(df)


if __name__ == '__main__':
    job()
