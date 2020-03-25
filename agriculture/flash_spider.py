#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import uuid
import pyamf
import datetime
from pyamf import remoting
from pyamf.flex import messaging
import pandas as pd


# charles 抓取 flash 数据包  分析数据包数据
class HqPara:
    def __init__(self):
        self.marketInfo = None
        self.breedInfoDl = None
        self.breedInfo = None
        self.province = None


# 注册自定义的Body参数类型，这样数据类型com.itown.kas.pfsc.report.po.HqPara就会在后面被一并发给服务端
# 否则服务端就可能返回参数不是预期的异常Client.Message.Deserialize.InvalidType
pyamf.register_class(HqPara, alias='com.itown.kas.pfsc.report.po.HqPara')

# 构造flex.messaging.messages.RemotingMessage消息
msg = messaging.RemotingMessage(messageId=str(uuid.uuid1()).upper(),
                                clientId=str(uuid.uuid1()).upper(),
                                operation='getHqSearchData',
                                destination='reportStatService',
                                timeToLive=0,
                                timestamp=0)


# https://blog.csdn.net/imiser2016/article/details/79231783   推荐参考博客
# 第一个是查询参数 第二个是页数 第三个是控制每页显示的数量（默认每页只显示15条）但爬取的时候可以一下子设置成全部的数量 构造请求数据
def get_request_data(page_num, total):
    msg.body = [HqPara(), str(page_num), str(total)]
    msg.headers['DSEndpoint'] = None
    msg.headers['DSId'] = str(uuid.uuid1()).upper()
    # 按AMF协议编码数据
    req = remoting.Request('null', body=(msg,))
    env = remoting.Envelope(amfVersion=pyamf.AMF3)
    env.bodies = [('/1', req)]
    data = bytes(remoting.encode(env).read())
    return data


# 返回一个请求的数据格式
def get_response(data):
    url = 'http://jgsb.agri.cn/messagebroker/amf'
    res = requests.post(url, data, headers={'Content-Type': 'application/x-amf'})
    return res.content


def get_content(response):
    amf_parse_info = remoting.decode(response)
    # 数据总条数
    total = amf_parse_info.bodies[0][1].body.body[3]
    info = amf_parse_info.bodies[0][1].body.body[0]
    return total, info


def store2df(info):
    df = pd.DataFrame(info)
    df.to_csv(datetime.datetime.now().strftime('%Y-%m-%d') + '全国农产品指数的抓取.csv', index=0, encoding='utf-8')


def job():
    total_num, info = get_content(get_response(get_request_data(1, 15)))
    num, data = get_content(get_response(get_request_data(1, total_num)))
    store2df(data)


if __name__ == '__main__':
    job()
