#!/usr/bin/python
# -*- coding: UTF-8 -*-

from selenium import webdriver


from selenium.webdriver.common.keys import Keys
import time


def main():
    '''

     chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(chrome_options=chrome_options)
    driver.get("https://www.baidu.com")
    print(driver.page_source)
    driver.close()

    '''

    driver = webdriver.Chrome()
    driver.get("http://www.baidu.com")
    assert u'百度' in driver.title
    elem = driver.find_element_by_name("wd")
    elem.clear()
    elem.send_keys(u'网络爬虫')
    elem.send_keys(Keys.RETURN)
    time.sleep(3)
    assert u"网络爬虫." not in driver.page_source
    driver.close()


if __name__ == '__main__':
    main()
