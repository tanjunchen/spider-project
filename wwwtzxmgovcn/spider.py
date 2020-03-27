# !/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import random
import time
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# 禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def job():
    headers = {
        'Host': 'www.tzxm.gov.cn:8081',
        'Connection': 'keep-alive',
        'Content-Length': '158',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'Origin': 'https://www.tzxm.gov.cn:8081',
        'Upgrade-Insecure-Requests': '1',
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Referer': 'https://www.tzxm.gov.cn:8081/tzxmspweb/portalopenPublicInformation.do?method=queryExamineAll',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=C3F036FB21F97A74B8F7714D55055BC8-n1:86; yfx_c_g_u_id_10005542=_ck19031117303413000125179104213; yfx_f_l_v_t_10005542=f_t_1552296634264__r_t_1552296634264__v_t_1552296634264__r_c_0; GJXXZX_HLW=91349450; SECTOKEN=7167049198324156243'
    }
    url = 'https://www.tzxm.gov.cn:8081/tzxmspweb/portalopenPublicInformation.do?method=queryExamineAll'
    s = requests.session()
    s.keep_alive = False
    df = pd.DataFrame()
    # total 205132   205067
    for i in range(111, 501):
        dd = get_content(i, url, headers)
        # df = df.append(dd)
        dd.to_csv("result/result.csv", index=False, header=False, encoding='utf-8', mode='a')


def get_content(i, url, headers):
    data = {
        'orderFlag': '',
        'serachFlag': '',
        'pageSize': 10,
        'pageNo': i,
        'apply_project_name': '',
        'project_area': '03',
        'project_name': '',
        'itemname': '',
        'dept_name': '',
        'apply_date_begin': '2016-01-01',
        'apply_date_end': ''
    }
    response = requests.post(url, headers=headers, data=data, verify=False, timeout=40)
    sleep_time = random.randint(4, 6)
    time.sleep(sleep_time)
    response.encoding = 'utf-8'
    dd = pd.read_html(response.text)[0]
    print("正在抓取第", i, "页数据,结果为")
    print(dd)
    print("休眠", sleep_time, "秒")
    if i > 1:
        return dd.loc[1:]
    else:
        return dd.loc[0:]


if __name__ == '__main__':
    job()
