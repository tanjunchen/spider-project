#!/usr/bin/env python
# -*- coding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import random
import time


def test_zcool(tracks):
    chrome_options = Options()
    chrome_options.add_argument('--proxy-server=127.0.0.1:8080')
    browser = webdriver.Chrome()
    # browser = webdriver.Chrome(executable_path="C:\\Windows\\System32\\chromedriver_handle_js.exe", chrome_options=chrome_options)
    # chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    browser.get("https://passport.zcool.com.cn/regPhone.do?appId=1006&cback=https://my.zcool.com.cn/focus/activity")
    WebDriverWait(browser, 10)
    # 找到滑块span
    need_move_span = browser.find_element_by_xpath('//*[@id="nc_1_n1t"]/span')
    # 模拟按住鼠标左键
    ActionChains(browser).click_and_hold(need_move_span).perform()
    for x in tracks:  # 模拟人的拖动轨迹
        # print(x)
        ActionChains(browser).move_by_offset(xoffset=x, yoffset=random.randint(1, 3)).perform()
    time.sleep(2)
    ActionChains(browser).release().perform()  # 释放左键
    WebDriverWait(browser, 5, 0.5).until(ec.presence_of_element_located((By.ID, 'nc_1__scale_text')))
    # info_text = browser.find_element_by_id("//*[@id='nc_1__scale_text']")
    # print(info_text)
    # 截屏
    # driver.save_screenshot('test.png')
    browser.close()


def get_track(distance):
    '''
    拿到移动轨迹 模仿人的滑动行为 先匀加速后匀减速
    匀变速运动基本公式：
    ①v=v0+at
    ②s=v0t+(1/2)at²
    ③v²-v0²=2as
    :param distance: 需要移动的距离
    :return: 存放每0.2秒移动的距离
    '''
    # 初速度
    v = 0
    # 单位时间为0.1s来统计轨迹 轨迹即0.1内的位移
    t = 0.08
    # 位移/轨迹列表 列表内的一个元素代表0.1s的位移
    tracks = []
    # 当前的位移
    current = 0
    # 到达mid值开始减速
    mid = distance * 4 / 5
    distance += 10  # 先滑过一点 最后再反着滑动回来
    while current < distance:
        if current < mid:
            # 加速度越小 单位时间的位移越小 模拟的轨迹就越多越详细
            a = 2  # 加速运动
        else:
            a = -3  # 减速运动
        # 初速度
        v0 = v
        # 0.2秒时间内的位移
        s = v0 * t + 0.5 * a * (t ** 2)
        # 当前的位置
        current += s
        # 添加到轨迹列表
        tracks.append(round(s))
        # 速度已经达到v,该速度作为下次的初速度
        v = v0 + a * t
    # 反着滑动到大概准确位置
    for i in range(3):
        tracks.append(-2)
    for i in range(4):
        tracks.append(-1)
    return tracks


if __name__ == '__main__':
    test_zcool(get_track(294))
