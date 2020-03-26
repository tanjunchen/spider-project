#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pandas as pd
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait


def get_data(url):
    browser = webdriver.Chrome()
    # chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--headless')
    # browser = webdriver.Chrome(chrome_options=chrome_options)
    # browser.maximize_window()  # 最大化窗口,可以选择设置
    wait = WebDriverWait(browser, 10)
    browser.get(url)
    try:
        page = browser.find_element_by_css_selector('.next+ a')  # next节点后面的a节点
    except:
        page = browser.find_element_by_css_selector('.at+ a')
    i = 1
    if page:
        page = int(page.text)
    while i <= page:
        # 确定页数输入框
        input = wait.until(EC.presence_of_element_located(
            (By.XPATH, '//*[@id="PageContgopage"]')))
        input.click()
        input.clear()
        input.send_keys(i)
        submit = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '#PageCont > a.btn_link')))
        submit.click()
        # 确认成功跳转到输入框中的指定页
        wait.until(EC.text_to_be_present_in_element(
            (By.CSS_SELECTOR, '#PageCont > span.at'), str(i)))
        element = browser.find_element_by_css_selector('#dt_1')
        td_content = element.find_elements_by_tag_name("td")  # 进一步定位到表格内容所在的td节点
        print("正在爬取" + url + "第" + str(i) + "页")
        lst = []  # 存储为list
        for td in td_content:
            lst.append(td.text)
        col = len(element.find_elements_by_css_selector('tr:nth-child(1) td'))
        # 通过定位一行td的数量，可获得表格的列数，然后将list拆分为对应列数的子list
        lst = [lst[i:i + col] for i in range(0, len(lst), col)]
        # 原网页中打开"详细"链接可以查看更详细的数据，这里我们把url提取出来，方便后期查看
        lst_link = []
        links = element.find_elements_by_css_selector('#dt_1 a.red')
        for link in links:
            url = link.get_attribute('href')
            lst_link.append(url)
        lst_link = pd.Series(lst_link)
        df_table = pd.DataFrame(lst)
        df_table['url'] = lst_link
        df_table.to_csv("data.scv", mode='a', index=False)
        i = i + 1


def get_url():
    # 重构url
    # 1 设置财务报表获取时期
    year = int(float(input('请输入要查询的年份(四位数2007-2018)：  ')))
    # int表示取整，里面加float是因为输入的是str，直接int会报错，float则不会
    while year < 2007 or year > 2018:
        year = int(float(input('年份数值输入错误，请重新输入：')))
    quarter = int(float(input('请输入小写数字季度(1:1季报，2-年中报，3：3季报，4-年报)：  ')))
    while quarter < 1 or quarter > 4:
        quarter = int(float(input('季度数值输入错误，请重新输入：  ')))
    # 转换为所需的quarter 两种方法,2表示两位数，0表示不满2位用0补充
    quarter = '{:02d}'.format(quarter * 3)
    # quarter = '%02d' %(int(month)*3)
    date = '{}{}'.format(year, quarter)
    # 2 设置财务报表种类
    tables = int(
        input('请输入查询的报表种类对应的数字(1-业绩报表；2-业绩快报表：3-业绩预告表；4-预约披露时间表；5-资产负债表；6-利润表；7-现金流量表):  '))
    dict_tables = {1: '业绩报表', 2: '业绩快报表', 3: '业绩预告表',
                   4: '预约披露时间表', 5: '资产负债表', 6: '利润表', 7: '现金流量表'}
    dict = {1: 'yjbb', 2: 'yjkb/13', 3: 'yjyg',
            4: 'yysj', 5: 'zcfz', 6: 'lrb', 7: 'xjll'}
    category = dict[tables]
    # 3 设置url
    url = 'http://data.eastmoney.com/{}/{}/{}.html'.format('bbsj', date, category)
    print(url)  # 测试输出的url


def run():
    pass


if __name__ == '__main__':
    get_data('http://data.eastmoney.com/bbsj/201806/xjll.html')
