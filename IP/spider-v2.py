#!/usr/bin/env python
# -*- coding: utf-8 -*-

from lxml import etree
import requests


def spider():
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
    html = etree.HTML(res.text)
    # 类型list
    type = html.xpath("//td/a[@class='href' and contains(@title,'http')]/text()")
    # 假端口list
    fake_port_list = [i[5:] for i in html.xpath('//td[@class="ip"]/span[last()]/@class')]

    alpha = 'ABCDEFGHIZ'
    real_port = []
    for fake_port in fake_port_list:
        num = ''
        for i in fake_port:
            num += str(alpha.index(i))
        real_port.append(str(int(num) // 8))

    tds = html.xpath(".//table[@class='table table-hover']/tbody/tr/td[1]")
    # ip_list list
    ip_list = []
    for td in tds:
        ip = "".join(td.xpath("./*[not(contains(@style,'none')) and not(contains(@class,'port'))]/text()"))
        ip_list.append(ip)
    # ip + port
    ip_port = [i[0] + ':' + i[1] for i in list(zip(ip_list, real_port))]
    result = []
    for index in range(len(ip_port)):
        result.append({type[index]: ip_port[index]})
    print(result)


if __name__ == '__main__':
    spider()
