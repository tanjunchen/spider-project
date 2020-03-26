#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
import time


def get_data(url):
    browser = webdriver.Chrome()
    WebDriverWait(browser, 10)
    browser.get(url)
    WebDriverWait(browser, 5, 0.5).until(EC.presence_of_element_located((By.ID, 'sidemenu')))
    above = browser.find_element_by_css_selector(
        "#sidemenu > div > div.level-list > ul > li.sub-items.menu-hsindex-wrapper")
    ActionChains(browser).move_to_element(above).perform()
    lis = browser.find_elements_by_xpath("//*[@id='sidemenu']/div/div[2]/ul/li[7]/div/ul//li")
    for li in lis:
        print(li)
        if "指数成份" in li.text:
            # li.click()
            # get_page_num(browser, "指数成份")
            print()
        elif "上证系列指数" in li.text:
            # li.click()
            # get_page_num(browser, "上证系列指数")
            print()
        elif "深证系列指数" in li.text:
            li.click()
            get_page_num(browser, "深证系列指数")


def get_page_num(browser, name):
    wait = WebDriverWait(browser, 10)
    try:
        wait.until(EC.presence_of_element_located((By.ID, 'main-table_next')))
        page = int(browser.find_element_by_xpath("//*[@id='main-table_paginate_page']/a[last()]").text)
    except Exception as e:
        print(e)
        page = 0
    i = 1
    df = pd.DataFrame()
    while i <= page:
        input_num = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@class="paginate_input"]')))
        input_num.click()
        input_num.clear()
        input_num.send_keys(i)
        submit = wait.until(EC.element_to_be_clickable(
            (By.XPATH, '//*[@class="paginte_go"]')))
        submit.click()
        data = pd.read_html(browser.page_source, converters={'代码': str})[0]
        data.drop(['序号'], axis=1, inplace=True)
        data['代码'].astype(str)
        print("正在爬取第" + str(i) + "页")
        df = df.append(data)
        i = i + 1
        time.sleep(2)
    df['指数'] = name
    df.to_csv(datetime.now().strftime('%Y%m%d') + name + ".csv", index=False)
    print(name, "抓取数据成功")
    browser.implicitly_wait(10)


if __name__ == '__main__':
    get_data('http://quote.eastmoney.com/center/boardlist.html#boards-BK01501')
    # search_next = browser.find_element_by_css_selector("#main-table_next")
    # while search_next.is_enabled():
    #     print("点击下一页")
    #     search_next.click()
    #     print(pd.read_html(browser.page_source)[0])
