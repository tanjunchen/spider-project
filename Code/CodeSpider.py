#!/usr/bin/python
# -*- coding: UTF-8 -*-

'''
爬取国家统计局行政编码的数据
'''

from multiprocessing import Pool, cpu_count
import random
import requests
from bs4 import BeautifulSoup as bs
import time
import json
import os

USER_AGENTS = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
]

# 目标列表
url = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2017/"
province = ["11", "12", "13", "14", "15", "21", "22", "23", "31", "32", "33", "34", "35", "36", "37",
            "41", "42", "43", "44", "45", "46", "50", "51", "52", "53", "54", "61", "62", "63", "64", "65"]

'''
断点重新尝试6次 
'''


def get_content(url, retires=6):
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        res = requests.get(url, headers=headers, timeout=20)
        res.encoding = 'gbk'
        content = bs(res.text, "lxml")
        return content
    except Exception as e:
        if retires > 0:
            time.sleep(1)
            print(url)
            print('requests fail retry the last th' + str(retires) + '  ' + time.ctime())
            return get_content(url, retires - 1)
        else:
            print("retry fail!")
            print("error: %s" % e + "   " + url)
            return  # 返回空值 程序运行报错停止


def get_citys(url, n):
    city = {}
    content = get_content(url + n + ".html")
    for j in content.select(".citytr "):
        id = str(j.select('td')[0].text)  # 130100000000
        city[id[0:4]] = {'qhdm': id, 'name': j.select('td')[1].text, 'cxfldm': '0'}
    return city


def get_area(url, lists):
    county = {}
    for i in lists:
        content = get_content(url + i[0:2] + '/' + i + '.html')
        for j in content.select('.countytr '):
            # print(j)
            id = str(j.select('td')[0].text)  # 130201000000
            county[id[0:6]] = {'qhdm': id, 'name': j.select('td')[1].text, 'cxfldm': '0'}
    return county


def get_town(url, lists):
    town = {}
    for i in lists:
        # print(url+i[0:2]+'/'+i[2:4]+'/'+i+'.html')
        content = get_content(url + i[0:2] + '/' + i[2:4] + '/' + i + '.html')
        for j in content.select('.towntr '):
            # print(j)
            id = str(j.select('td')[0].text)  # 130202001000
            town[id[0:9]] = {'qhdm': id, 'name': j.select('td')[1].text, 'cxfldm': '0'}  # 130202001
    return town


def get_village(url, lists):
    village = {}
    for i in lists:
        # print(url+i[0:2]+'/'+i[2:4]+'/'+i[4:6]+'/'+i+'.html')
        content = get_content(url + i[0:2] + '/' + i[2:4] + '/' + i[4:6] + '/' + i + '.html')
        for j in content.select('.villagetr '):
            # print(j)
            id = str(j.select('td')[0].text)  # 110101001001
            village[id[0:12]] = {'qhdm': id, 'name': j.select('td')[2].text,
                                 'cxfldm': j.select('td')[1].text}  # 110101001001
    return village


def spider(url, n):
    city = get_citys(url, n)
    print(n + ' city finished!')

    county = get_area(url, city)
    print(n + ' county finished!')

    town = get_town(url, county)
    print(n + ' town finished!')

    village = get_village(url, town)
    print(n + ' village finished!')

    print(n + " crawl finished ")
    return city, county, town, village


def down_load(city, county, town, village, n):
    path = 'code/2017_ ' + n + '.txt'
    dic = {**city, **county, **town, **village}  # 字典合并
    for i in dic.values():
        with open(path, 'a', encoding='utf-8') as f:
            try:
                f.write(i['qhdm'] + ',' + i['name'] + ',' + i['cxfldm'] + '/n')
            except Exception as e:
                print(e)
    print(n + " write finished!")


def run_pro(url, n):
    print("Crawling ", n, ' is running')
    (city, county, town, village) = spider(url, n)
    down_load(city, county, town, village, n)
    print("Crawling ", n, ' ended')


def job():
    start_time = time.time()
    p = Pool(min(province.__len__(), cpu_count() * 4))
    for i in province:
        p.apply_async(run_pro, args=(url, i))
    print('Waiting for all subprocesses done ...')
    p.close()  # 关闭进程池
    p.join()  # 等待开辟的所有进程执行完后，主进程才继续往下执行
    print('All subprocesses done')
    print(time.time() - start_time)


def count_txt():
    count = 0
    # Note that the path of file, it will cause some problems now.
    DIR = "/Spider/Code/code"
    for i in os.listdir(DIR):
        with open(DIR + "/" + i, 'r', encoding='utf-8') as f:
            while True:
                buffer = f.read(1014 * 8912)
                if not buffer:
                    break
                count += buffer.count("/n")
    return count


def get_all_data_from_txt():
    DIR = "Spider/Code/code"
    lis = os.listdir(DIR)
    with open("code/all.txt", 'a', encoding='utf-8') as fl:
        for i in lis:
            # print(i)
            with open(DIR + "/" + i, 'r', encoding='utf-8') as f:
                buffer = f.read()
                fl.write(buffer)


'''
添加随机headers 断点重新爬取相关信息(重复尝试6次) 使用多进程的技术 采用 gbk 编码 
'''
if __name__ == '__main__':
    # job()
    get_all_data_from_txt()
    print(count_txt())
