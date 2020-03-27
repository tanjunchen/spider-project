#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import requests
# from fake_useragent import UserAgent
#
# ua = UserAgent()
#
# for i in range(100):
#     content = requests.get("https://blog.csdn.net/tanjunchen/article/details/84995808",
#                            headers={'User-Agent': ua.random})
#     print("正在请求", i)

import requests
from bs4 import BeautifulSoup
import multiprocessing
import time

success_num = 0

CONSTANT = 0


def getProxyIp():
    global CONSTANT
    proxy = []
    for i in range(1, 80):
        print(i)
        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36'}
        r = requests.get('http://www.xicidaili.com/nt/{0}'.format(str(i)), headers=header)
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        table = soup.find('table', attrs={'id': 'ip_list'})
        tr = table.find_all('tr')[1:]

        # 解析得到代理ip的地址，端口，和类型
        for item in tr:
            tds = item.find_all('td')
            print(tds[1].get_text())
            temp_dict = {}
            kind = tds[5].get_text().lower()
            # exit()
            if 'http' in kind:
                temp_dict['http'] = "http://{0}:{1}".format(tds[1].get_text(), tds[2].get_text())
            if 'https' in kind:
                temp_dict['https'] = "https://{0}:{1}".format(tds[1].get_text(), tds[2].get_text())

            proxy.append(temp_dict)
    return proxy


def brash(proxy_dict):
    header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '
                            'AppleWebKit/537.36 (KHTML, like Gecko) '
                            'Ubuntu Chromium/44.0.2403.89 '
                            'Chrome/44.0.2403.89 '
                            'Safari/537.36',
              'Referer': 'https://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=1&tn=baidu&wd=csdn%20%E6%80%9D%E6%83%B3%E7%9A%84%E9%AB%98%E5%BA%A6%20csdnzouqi&oq=csdn%20%E6%80%9D%E6%83%B3%E7%9A%84%E9%AB%98%E5%BA%A6&rsv_pq=fe7241c2000121eb&rsv_t=0dfaTIzsy%2BB%2Bh4tkKd6GtRbwj3Cp5KVva8QYLdRbzIz1CCeC1tOLcNDWcO8&rqlang=cn&rsv_enter=1&rsv_sug3=11&rsv_sug2=0&inputT=3473&rsv_sug4=3753'

              }
    try:
        r = requests.get("xxxx", headers=header,
                         proxies=proxy_dict, timeout=10)
        print(r.status_code)
    except BaseException as e:
        print("failed", e)
    else:
        print("successful")
    time.sleep(0.5)
    return None


if __name__ == '__main__':
    i = 0
    t = 0
    final = 10  # 输入数字代表要获取多少次代理ip
    while t < final:
        t += 1
        proxies = getProxyIp()  # 获取代理ip网站上的前12页的ip
        # 为了爬取的代理ip不浪费循环5次使得第一次的不能访问的ip尽可能利用
        # print CONSTANT
        for i in range(5):
            i += 1
            # 多进程代码开了32个进程
            pool = multiprocessing.Pool(processes=32)
            results = []
            for i in range(len(proxies)):
                results.append(pool.apply_async(brash, (proxies[i],)))
            for i in range(len(proxies)):
                results[i].get()
            pool.close()
            pool.join()
        i = 0
        time.sleep(10)
