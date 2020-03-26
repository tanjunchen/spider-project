#!/usr/bin/python
# -*- coding: UTF-8 -*-

import requests
from fake_useragent import UserAgent
import pandas as pd
from lxml import etree
import re

ua = UserAgent()
headers = {"User-Agent": ua.random}


def job():
    url = "https://www.trjcn.com/investor_data.html?type=1&page={0}"
    headers.pop("Referer", url.format(1))
    total = get_total(url, headers)
    dates = []
    events = []
    sources = []
    destinations = []
    moneys = []
    realms = []
    for page in range(1, total + 1):
        res = requests.get(url.format(page), headers=headers)
        res.encoding = "utf-8"
        print("正在抓取第", str(page) + "页")
        if res.ok:
            html = etree.HTML(res.text)
            if html is not None:
                trs = html.xpath("//div[@class='fn-left list']/table//tr")
                for i in range(1, len(trs)):
                    tds = trs[i].xpath(".//td")
                    date = "-".join(tds[0].xpath(".//text()"))
                    event = "-".join(tds[1].xpath(".//text()"))
                    source = "-".join(tds[2].xpath(".//text()"))
                    destination = "-".join(tds[3].xpath(".//text()"))
                    money = "-".join(tds[4].xpath(".//text()"))
                    realm = "-".join(tds[5].xpath(".//text()"))
                    dates.append(date)
                    events.append(event)
                    sources.append(source)
                    destinations.append(destination)
                    moneys.append(money)
                    realms.append(realm)
                    print("正在抓取第", str(page) + "页", date, event, source, destination, money, realm)
    all_data = []
    for i in range(len(events)):
        all_data.append([dates[i], events[i], sources[i], destinations[i], moneys[i], realms[i]])
    df = pd.DataFrame(all_data)
    new_col = ['日期', '融资事件', '融资方', '投资方', '金额与轮次', '融资领域']
    df.columns = new_col
    df.to_csv("all_data.csv", encoding="utf-8", index=False)


def get_total(url, header):
    headers.pop("Referer", url)
    res = requests.get(url, headers=header)
    res.encoding = "utf-8"
    html = etree.HTML(res.text)
    total = re.findall('\d+', html.xpath("//div[@class='paging fn-right']//a[last()]/@href")[0])[-1]
    return int(total)


if __name__ == '__main__':
    job()
