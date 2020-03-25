#!/usr/bin/python
# -*- coding: UTF-8 -*-

import re
import requests
import pytesseract
from PIL import Image
from selenium import webdriver
from fake_useragent import UserAgent
from lxml import etree
from urllib import parse
import pandas as pd
from datetime import datetime

ua = UserAgent()
headers = {"User-Agent": ua.random,
           "Referer": "http://gz.ziroom.com/"}


class ZiRoom(object):

    def __init__(self):
        self.driver = webdriver.Chrome()
        self.all_data = []

    def get_content(self, name):
        div_list = self.driver.find_elements_by_xpath('//*[@id="houseList"]/li')
        number = self.get_image_number()
        print(number)
        for div in div_list[1:]:
            try:
                price_list = []
                # 如果网页中的值不存在 则可能会存在部分数据丢失 也就是空数据丢失 基本不会有什么影响
                for i in range(2, 6):
                    start_price = \
                        div.find_element_by_xpath('.//div[3]/p/span[{}]'.format(i)).get_attribute('style').split(' ')[
                            1].replace('-', '').replace('px', '')
                    price = number[int(int(start_price) / 30)]
                    price_list.append(price)
                price = '{}元/每月'.format(''.join(price_list))
                title = div.find_element_by_xpath('.//div[2]/h3/a').text.replace(' ', '')
                location = div.find_element_by_xpath('.//div[2]/div//p[2]/span').text.replace(' ', '')
                area = div.find_element_by_xpath('.//div[2]/div/p/span').text.replace(' ', '')
                self.all_data.append([name, title, price, location, area])

            except BaseException as e:
                print(e)
                pass

    def get_image_number(self):
        '''
        获取价格的图片  并且通过 pytesseract 识别图片 解成数字列表
        :return:
        '''
        # 价格数据来源于js 加载的图片
        html = self.driver.execute_script("return document.documentElement.outerHTML")
        photo = re.findall('var ROOM_PRICE = {"image":"(//.*?.png)"', html)[0]
        image = requests.get('http:' + photo).content
        f = open('price.png', 'wb')
        f.write(image)
        f.close()
        num = []
        '''
        pytesseract psm 选项参数
        0    Orientation and script detection (OSD) only.
        1    Automatic page segmentation with OSD.
        2    Automatic page segmentation, but no OSD, or OCR.
        3    Fully automatic page segmentation, but no OSD. (Default)
        4    Assume a single column of text of variable sizes.
        5    Assume a single uniform block of vertically aligned text.
        6    Assume a single uniform block of text.
        7    Treat the image as a single text line.
        8    Treat the image as a single word.
        9    Treat the image as a single word in a circle.
        10    Treat the image as a single character. 
        11    Sparse text. Find as much text as possible in no particular order.
        12    Sparse text with OSD.
        13    Raw line. Treat the image as a single text line
        '''
        number = pytesseract.image_to_string(Image.open("price.png"),
                                             config="-psm 8 -c tessedit_char_whitelist=1234567890")
        for i in number:
            num.append(i)
        return num

    def run(self):
        url = "http://www.ziroom.com/z/nl/z2-d23008614.html"
        res = requests.get(url, headers=headers)
        res.encoding = "utf-8"
        html = etree.HTML(res.text)
        name = html.xpath("//div[@class='selection_con']/dl[2]/dd/ul/li[position()>1]/span/a/text()")
        urls = html.xpath("//div[@class='selection_con']/dl[2]/dd/ul/li[position()>1]/span/a/@href")
        urls = [parse.urljoin(url, n) for n in urls]
        data = zip(name, urls)
        for k, v in data:
            print("正在获取", k, v)
            res = requests.get(v, headers=headers)
            res.encoding = "utf-8"
            html = etree.HTML(res.text)
            page_xpath = html.xpath("//span[@class='pagenum']/text()")
            if page_xpath:
                page_num = page_xpath[0].strip("/")
                if page_num:
                    for i in range(1, int(page_num) + 1):
                        # self.driver.implicitly_wait(10)
                        self.driver.get(v + '?p={}'.format(i))
                        self.get_image_number()
                        self.get_content(k)
            df = pd.DataFrame(self.all_data)
            df.columns = ['地区', '房源', '价格', '位置', '面积']
            df.to_csv("all_result_" + datetime.now().strftime('%Y-%m-%d') + ".csv", index=False, encoding="gbk")


if __name__ == '__main__':
    z = ZiRoom()
    z.run()
