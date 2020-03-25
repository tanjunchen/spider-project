#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urllib import request
from lxml import etree


# 构造函数，抓取第i页信息
def crow(i):
    #  构造第i页的网址
    url = 'https://movie.douban.com/top250?start=' + str(25 * i)
    #  发送请求，获得返回的html代码并保存在变量html中
    html = request.urlopen(url).read().decode('utf-8')
    # 将返回的字符串格式的html代码转换成xpath能处理的对象
    html = etree.HTML(html)
    # 先定位到li标签，datas是一个包含25个li标签的list，就是包含25部电影信息的list
    datas = html.xpath('//ol[@class="grid_view"]/li')
    a = 0
    for data in datas:
        data_title = data.xpath('div/div[2]/div[@class="hd"]/a/span[1]/text()')
        data_info = data.xpath('div/div[2]/div[@class="bd"]/p[1]/text()')
        data_quote = data.xpath('div/div[2]/div[@class="bd"]/p[2]/span/text()')
        data_score = data.xpath('div/div[2]/div[@class="bd"]/div/span[@class="rating_num"]/text()')
        data_num = data.xpath('div/div[2]/div[@class="bd"]/div/span[4]/text()')
        data_picurl = data.xpath('div/div[1]/a/img/@src')
        print("No: " + str(i * 25 + a + 1))
        print(data_title)
        # 保存电影信息到txt文件，下载封面图片
        with open('douban250.txt', 'a', encoding='utf-8')as f:
            # 封面图片保存路径和文件名
            picname = './top250/' + str(i * 25 + a + 1) + '.jpg'
            f.write("No: " + str(i * 25 + a + 1) + '\n')
            f.write(data_title[0] + '\n')
            f.write(str(data_info[0]).strip() + '\n')
            f.write(str(data_info[1]).strip() + '\n')
            # 因为发现有几部电影没有quote，所以这里加个判断，以免报错
            if data_quote:
                f.write(data_quote[0] + '\n')
            f.write(data_score[0] + '\n')
            f.write(data_num[0] + '\n')
            f.write('\n' * 3)
            # 下载封面图片到本地，路径为picname
            request.urlretrieve(data_picurl[0], filename=picname)
        a += 1


if __name__ == '__main__':
    for i in range(10):
        crow(i)
