#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool, Manager, cpu_count
import pandas as pd


def district():
    category = [1001, 1002, 1003, 2001, 2002, 2003, 6001, 6002, 6003, 7001, 7002, 7003, 4001, 4002, 4003]  # 歌手种类的值
    character = [-1, 0, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87,
                 88, 89, 90]  # 字母分类的值
    urls = []
    for i in category:
        for j in character:
            urls.append('http://music.163.com/discover/artist/cat?id=' + str(i) + '&initial=' + str(j))
    return urls


# 构造函数获取歌手信息
def get_artists(url):
    print("正在爬取", url)
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Connection': 'keep-alive',
               'Cookie': '_ntes_nnid=ff190d625e023a01cfe982c4b2655ae7,1539830921249; __f_=1539830920786; _ntes_nuid=ff190d625e023a01cfe982c4b2655ae7; Province=010; City=010; UM_distinctid=166bd3b501043a-0e88d239a22e2e-333b5602-1fa400-166bd3b5012ea5; ne_analysis_trace_id=1540773073367; s_n_f_l_n3=ff37fe845a016e7c1540773073375; _antanalysis_s_id=1540773073594; NNSSPID=86b7d2e419f34ab991b0cf1b1b04d2c8; vinfo_n_f_l_n3=ff37fe845a016e7c.1.0.1540773073372.0.1540773100782; _iuqxldmzr_=32; __utmc=94650624; __utmz=94650624.1540877287.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; WM_TID=nB5rNmUdgf9EQFVQEFM5PKRPekN%2Bkr5h; WM_NI=lvS79r%2B4x1qNpULxFk0Ig9bU5oYrCYPW54%2FJ%2B4gshAJQR92xAuCuHOxYf0%2B90EIlalwQMGvktjjwIJpRpMAK7d2GN%2FkaD%2Fz%2BVcHcCdD9aGXcxF6TE78IXYOn7lbLqeOCUkg%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6ee90ee3d9296a3a8d84fa9ef8aa7d85f838b8eaeee48a69ab8b5c161aceebe8aed2af0fea7c3b92ab0aa9dd8bc7ae9ecb8d2f446b48b8499c561f497a9dae15ff7b19793c2539489fbd5ae42e9eca994eb4b8a8f9edab17483ab8d96cb45bc87aad1aa33f49b878de468b58f83a8f561b8b6a698cc42b48ec0aec441f5bcfea8cd25a9bf8cd0f92196bd8884c27088e7afa8db69bc9afab5c83dfc9798bace66f5b4aeadf46f8d9d9b8ce637e2a3; JSESSIONID-WYYY=OVTx6HFR7vMidD4HQqowqGxeu2P4mRfH4hNscNonuQqbu9dsrgypYK1JxWu%2Fy%2FX7zd%2BZZAiRPyUPTSJIWRK0u1WBav370%2Fr70bqzHUPXWZtRqNSmnGgvZnv5Hh6%2FdotZlJ7FizdwiowCWHzXXN087JvvCoi6FTIvrYSumu2Q86QN87jZ%3A1540891269502; __utma=94650624.2136174994.1540877287.1540877287.1540890955.2; __utmb=94650624.2.10.1540890955',
               'Host': 'music.163.com',
               'Referer': 'http://music.163.com/',
               'Upgrade-Insecure-Requests': '1',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/66.0.3359.181 Safari/537.36'}
    r = requests.get(url)
    r.encoding = 'utf-8'
    soup = BeautifulSoup(r.text, 'lxml')
    result = []
    for artist in soup.find_all('a', attrs={'class': 'nm nm-icn f-thide s-fc0'}):
        artist_name = artist.string
        artist_id = artist['href'].replace('/artist?id=', '').strip()
        print(artist_id, artist_name)
        result.append([artist_id, artist_name])
    df = pd.DataFrame(result_list)
    df.to_csv("data/net_source.csv", index=False, encoding='utf-8', mode="a+", header=False)


if __name__ == '__main__':
    urls = district()
    pool_size = min(cpu_count() * 4, urls.__len__())
    result_list = Manager().list()
    pool = Pool(pool_size)
    for url in urls:
        print(url)
        pool.apply_async(get_artists, args=(url,), error_callback=lambda e: print(e))
    pool.close()
    pool.join()
