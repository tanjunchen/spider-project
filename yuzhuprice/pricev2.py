#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import Pool, Manager
import requests
from fake_useragent import UserAgent
from lxml import etree
import pandas as pd

ua = UserAgent()


def error_func(x):
    print(x)
    print('进程退出')


def spider(task_queue, data_queue):
    base_url = 'http://www.yuzhuprice.com/CTIExponent.jspx?'
    while True:
        task = task_queue.get()
        if task == 'exit':
            task_queue.put(task)
            return
        for number in range(1, 99):
            data = {
                'page.curPage': str(number),
                'pattern': '0',
                'expName': task,
                'periods': 'day'
            }
            res = requests.get(url=base_url, headers={'User-Agent': ua.random}, params=data)
            data = pd.read_html(res.text)[1]
            data_queue.put(data)
            print(str(task) + '=======>' + str(number))
        print(str(task) + '===========>done')


def dump(data_queue):
    number = 0
    while True:
        item = data_queue.get(timeout=120)
        if number > 0:
            item.to_csv('tree.csv', index=0, encoding='gbk', mode='a', header=0)
        else:
            item.to_csv('tree.csv', index=0, encoding='gbk', mode='a')
        number += 1


def thr(task_queue, data_queue):
    executor = ThreadPoolExecutor(max_workers=8)
    for i in range(8):
        executor.submit(spider, task_queue, data_queue)
    executor.shutdown()


def job():
    url = 'http://www.yuzhuprice.com/CTIExponent.jspx?'
    res = requests.get(url=url, headers={'User-Agent': ua.random})
    html = etree.HTML(res.text)
    task_list = html.xpath("//div[@id='cat_list']//li//text()")
    task_list = [i.replace(' |- ', '') for i in task_list]
    task_queue = Manager().Queue()
    data_queue = Manager().Queue()
    for i in task_list:
        task_queue.put(i)
    task_queue.put('exit')
    pool_size = 5
    p = Pool(pool_size)
    print(datetime.now())
    p.apply_async(dump, args=(data_queue,), error_callback=error_func)
    for _ in range(pool_size - 1):
        p.apply_async(thr, args=(task_queue, data_queue), error_callback=error_func)
    p.close()
    p.join()
    print(datetime.now())


if __name__ == '__main__':
    job()
