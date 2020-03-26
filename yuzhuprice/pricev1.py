#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import pandas as pd
from lxml import etree
import os
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from multiprocessing import Pool, Manager
from fake_useragent import UserAgent


# ua = UserAgent(verify_ssl=False)


def get_category():
    url = "http://www.yuzhuprice.com/CTIExponent.jspx"
    # headers = {'User-Agent': ua.random}
    res = requests.get(url, timeout=60)
    if res.ok:
        res.encoding = 'utf-8'
        content = etree.HTML(res.text)
        name = content.xpath("//div[@id='cat_list']//ul//li//a/text()")
        exp_names = [i.replace(" |- ", "") for i in name]
        print("expName：", exp_names)
        return exp_names


def get_content(url, params):
    res = requests.get(url, params, timeout=60)
    res.encoding = 'utf-8'
    if res.ok:
        data = pd.DataFrame(pd.read_html(res.text)[-1])
        return data
    return None


def to_csv(data_queue):
    csv = datetime.now().strftime("%Y-%m-%d") + "data.csv"
    while True:
        data = data_queue.get(timeout=120)
        if not os.path.exists(csv):
            data.columns = ["分类", "统计类型", "指数", "环比", "环比涨跌幅", "同比", "同比涨跌幅", "发布时间"]
            data.to_csv(csv, index=False, encoding="gbk")
        else:
            data.to_csv(csv, index=False, encoding="gbk", mode="a+", header=False)


def yu_zhu_price(task_queue, data_queue):
    url = "http://www.yuzhuprice.com/CTIExponent.jspx?"
    params = {
        "page.curPage": "2",
        "pattern": "0",
        "expName": "中国木材价格指数",
        "periods": "day",
    }
    while True:
        task = task_queue.get()
        if task == 'exit':
            task_queue.put(task)
            return
        params.__setitem__("expName", task)
        page = 1
        flag = True
        while flag:
            params.__setitem__("page.curPage", page)
            data = get_content(url, params=params)
            if len(data) > 1:
                page = page + 1
                data = data.loc[1:, ]
                data_queue.put(data)
            else:
                flag = False
            print("正在抓取===>", page)
            print("数据为", data)


def error_func(x):
    print(x)
    print('进程退出')


def thr(task_queue, data_queue):
    executor = ThreadPoolExecutor(max_workers=8)
    for i in range(8):
        executor.submit(yu_zhu_price, task_queue, data_queue)
    executor.shutdown()


def job():
    exp_names = get_category()
    task_queue = Manager().Queue()
    data_queue = Manager().Queue()
    for exp_name in exp_names:
        task_queue.put(exp_name)
    task_queue.put('exit')
    start_time = datetime.now()
    print("开始时间：", start_time)
    pool_size = 5
    p = Pool(pool_size)
    p.apply_async(to_csv, args=(data_queue,), error_callback=error_func)
    for _ in range(pool_size - 1):
        p.apply_async(thr, args=(task_queue, data_queue), error_callback=error_func)
    p.close()
    p.join()
    print("结束时间：", start_time, "此次共消耗", datetime.now() - start_time, "秒")


if __name__ == '__main__':
    job()
