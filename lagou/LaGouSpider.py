#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import time

import requests
from openpyxl import Workbook
import pymysql.cursors


def get_conn():
    conn = pymysql.connect(host='localhost',
                           user='root',
                           password='x',
                           db='lagou',
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    return conn


def insert(conn, info):
    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO `python` (`shortname`, `fullname`, `industryfield`, " \
                  "`companySize`, `salary`, `city`, `education`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, info)
        conn.commit()
        print("数据入库成功......")
    except Exception as e:
        print(e)
        conn.rollback()


def get_json(url, page, lang_name):
    '''返回当前页面的信息列表'''
    headers = {
        'Host': 'www.lagou.com',
        'Connection': 'keep-alive',
        'Content-Length': '23',
        'Origin': 'https://www.lagou.com',
        'X-Anit-Forge-Code': '0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'X-Anit-Forge-Token': 'None',
        'Referer': 'https://www.lagou.com/jobs/list_python?city=%E5%85%A8%E5%9B%BD&cl=false&fromSearch=true&labelWords=&suginput=',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7'
    }
    data = {'first': 'false', 'pn': page, 'kd': lang_name}
    json = requests.post(url, data, headers=headers, timeout=20).json()
    list_con = json['content']['positionResult']['result']
    info_list = []
    for i in list_con:
        info = [i.get('companyShortName', '无'), i.get('companyFullName', '无'), i.get('industryField', '无'),
                i.get('companySize', '无'), i.get('salary', '无'), i.get('city', '无'), i.get('education', '无')]
        info_list.append(info)
    return info_list  # 返回列表


def main():
    lang_name = 'python'
    wb = Workbook()  # 打开 excel 工作簿
    conn = get_conn()  # 建立数据库连接  不存数据库 注释此行
    for i in ['上海', '广州', '深圳', '杭州', '北京']:  # 五个城市
        page = 1
        ws1 = wb.active
        ws1.title = lang_name
        url = 'https://www.lagou.com/jobs/positionAjax.json?city={}&needAddtionalResult=false'.format(i)
        while page < 31:  # 每个城市30页信息
            info = get_json(url, page, lang_name)
            page += 1
            for row in info:
                insert(conn, tuple(row))  # 插入数据库，若不想存入 注释此行
                ws1.append(row)
            time.sleep(random.randint(40, 60))
    conn.close()  # 关闭数据库连接，不存数据库 注释此行
    wb.save('{}职位信息.xlsx'.format(lang_name))


if __name__ == '__main__':
    main()
