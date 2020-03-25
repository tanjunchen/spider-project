#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import requests
import itchat
from itchat.content import *
import time
from apscheduler.schedulers.background import BackgroundScheduler
import re
import json

KEY = 'xxx'


def get_response(msg):
    apiUrl = 'http://www.tuling123.com/openapi/api'
    data = {
        'key': KEY,
        'info': msg,
        'userid': 'wechat-robot',
    }
    try:
        r = requests.post(apiUrl, data=data).json()
        return r.get('text')
    except Exception as e:
        print(e)
        return


# 文件临时存储页
rec_tmp_dir = os.path.join(os.getcwd(), 'tmp/')

# 存储数据的字典
rec_msg_dict = {}

msg_dict = {}


def after_login():
    print("登录后调用")
    # 获取自己的用户信息，返回自己的属性字典
    # result = itchat.search_friends(name='龙淑宁')
    # print(result)
    # print("========================================================")
    # 根据姓名查找用户
    # user_info = itchat.search_friends(name='高群翔')
    # print(user_info)
    # print("========================================================")
    # if len(user_info) > 0:
    #     # 拿到用户名
    #     user_name = user_info[0]['UserName']
    #     # 发送文字信息
    #     itchat.send_msg('老铁你好啊！ 我是机器人哦 一起来聊天吧 ', user_name)
    #     content = get_weather()
    #     itchat.send_msg(content, user_name)
    # # 发送图片
    # time.sleep(10)
    # itchat.send_image('data/c3e20e1e5e2af7948b5afa3a9443a455.jpg', user_name)
    # # 发送文件
    # time.sleep(10)
    # itchat.send_file('data/1552568575305.gif', user_name)
    # 发送视频
    # time.sleep(10)
    # itchat.send_video('sport.mp4', user_name)
    # time.sleep(5)
    # itchat.send("文件助手你好哦", toUserName="filehelper")
    print("========================================================")
    # print("完整的群聊列表如下：")
    # rooms = itchat.get_chatrooms()
    # print(rooms)
    # 通过群聊名查找
    # chat_rooms = itchat.search_chatrooms(name='飘啊飘')
    # if len(chat_rooms) > 0:
    #     itchat.send_msg('我是Python脚本哦,大家好啊', chat_rooms[0]['UserName'])
    # 查找特定群聊
    # time.sleep(10)
    weather_schedule()
    # start_schedule()


def after_logout():
    print("退出后调用")


# 好友信息监听
@itchat.msg_register([TEXT, PICTURE, RECORDING, ATTACHMENT, VIDEO], isFriendChat=True)
def handle_friend_msg(msg):
    msg_id = msg['MsgId']
    msg_from_user = msg['User']['NickName']
    msg_content = ''
    # 收到信息的时间
    msg_time_rec = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    msg_create_time = msg['CreateTime']
    msg_type = msg['Type']

    if msg['Type'] == 'Text':
        msg_content = msg['Content']
    elif msg['Type'] == 'Picture' \
            or msg['Type'] == 'Recording' \
            or msg['Type'] == 'Video' \
            or msg['Type'] == 'Attachment':
        msg_content = r"" + msg['FileName']
        msg['Text'](rec_tmp_dir + msg['FileName'])
    rec_msg_dict.update({
        msg_id: {
            'msg_from_user': msg_from_user,
            'msg_time_rec': msg_time_rec,
            'msg_create_time': msg_create_time,
            'msg_type': msg_type,
            'msg_content': msg_content
        }
    })
    print("who:", msg_from_user, "createTime:", msg_create_time, "recvTime:", msg_time_rec, "content:", msg_type,
          msg_content)


# 群聊信息监听
@itchat.msg_register([TEXT, PICTURE, RECORDING, ATTACHMENT, VIDEO], isGroupChat=True)
def information(msg):
    msg_id = msg['MsgId']
    msg_from_user = msg['ActualNickName']
    msg_content = ''
    # 收到信息的时间
    msg_time_rec = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    msg_create_time = msg['CreateTime']
    msg_type = msg['Type']

    if msg['Type'] == 'Text':
        msg_content = msg['Content']
    elif msg['Type'] == 'Picture' \
            or msg['Type'] == 'Recording' \
            or msg['Type'] == 'Video' \
            or msg['Type'] == 'Attachment':
        msg_content = r"" + msg['FileName']
        msg['Text'](rec_tmp_dir + msg['FileName'])
    rec_msg_dict.update({
        msg_id: {
            'msg_from_user': msg_from_user,
            'msg_time_rec': msg_time_rec,
            'msg_create_time': msg_create_time,
            'msg_type': msg_type,
            'msg_content': msg_content
        }
    })
    print("who:", msg_from_user, "createTime:", msg_create_time, "recvTime:", msg_time_rec, "content:", msg_type,
          msg_content)


# 每隔五种分钟执行一次清理任务
def clear_cache():
    # 当前时间
    cur_time = time.time()
    # 遍历字典，如果有创建时间超过2分钟(120s)的记录，删除，非文本的话，连文件也删除
    for key in list(rec_msg_dict.keys()):
        if int(cur_time) - int(rec_msg_dict.get(key).get('msg_create_time')) > 600:
            if not rec_msg_dict.get(key).get('msg_type') == 'Text':
                file_path = os.path.join(rec_tmp_dir, rec_msg_dict.get(key).get('msg_content'))
                print(file_path)
                if os.path.exists(file_path):
                    os.remove(file_path)
            rec_msg_dict.pop(key)


def weather_schedule():
    user_info = itchat.search_friends(name='龙淑宁')
    if len(user_info) > 0:
        # 拿到用户名
        user_name = user_info[0]['UserName']
        print("用户名:", user_name)
    content = get_weather()
    itchat.send_msg(content, user_name)


# 开始轮询任务
def start_schedule():
    scheduler = BackgroundScheduler()
    scheduler.add_job(clear_cache, 'interval', minutes=20)
    scheduler.add_job(weather_schedule, 'interval', hours=24)
    scheduler.start()
    try:
        # 其他任务是独立的线程执行
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print('Exit The Job!')


@itchat.msg_register([NOTE])
def send_msg_helper(msg):
    print('收到一条提醒', msg)
    # 正则表达式搜索消息内容
    search_res = re.search("<!\[CDATA\[.* 撤回了一条消息\]\]", msg['Content'])
    if search_res is not None:
        # 获取消息的id
        revoke_msg_id = re.search("<msgid>(.*?)</msgid>", msg['Content']).group(1)
        old_msg = msg_dict.get(revoke_msg_id, {})
        # 小于11就是非文本消息
        if len(revoke_msg_id) < 11:
            pass
        else:
            msg_body = old_msg.get('msg_from')
            if isinstance(msg_body, str):
                msg_body += ' 撤回了一个文本消息'
                msg_body += old_msg.get('msg_time_rec')
                msg_body += "内容是["
                msg_body += old_msg.get('msg_content')
                msg_body += "]"
                # 将撤回消息发送到文件助手
                itchat.send(msg_body, toUserName='filehelper')
                # 删除字典旧消息
                msg_dict.pop(revoke_msg_id)
            else:
                print("非文本消息")
    else:
        print('不是撤回')


# 图灵机器人数据接口
@itchat.msg_register(itchat.content.TEXT)
def tuling_reply(msg):
    info = msg['Content'].encode('utf8')
    print("收到一条信息：", msg.text)
    # 图灵API接口
    api_url = 'http://openapi.tuling123.com/openapi/api/v2'
    # 接口请求数据
    data = {
        "reqType": 0,
        "perception": {
            "inputText": {
                "text": str(info)
            }
        },
        "userInfo": {
            "apiKey": KEY,
            "userId": "ctj"
        }
    }
    headers = {
        'Content-Type': 'application/json',
        'Host': 'openapi.tuling123.com',
        'User-Agent': 'Mozilla/5.0 (Wi`ndows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3486.0 '
                      'Safari/537.36 '
    }
    # 请求接口
    result = requests.post(api_url, headers=headers, json=data).json()
    # 提取text，发送给发信息的人
    reply_text = result['results'][0]['values']['text']
    print("图灵机器人回复:", reply_text)
    itchat.send_msg(reply_text, msg['FromUserName'])


def get_weather():
    city_sum = ['北京', '长沙', '茶陵']
    content = '嗨,小猪猪为你播报\n'
    for i in city_sum:
        url = 'http://api.map.baidu.com/telematics/v3/weather?' \
              'location=%s&output=json&ak=TueGDhCvwI6fOrQnLM0qmXxY9N0OkOiQ&callback=?' % i
        content = content + get_weather_content(url)
    return content


def get_weather_content(url):
    # 使用requests发起请求，接受返回的结果
    rs = requests.get(url)
    # 使用loads函数，将json字符串转换为python的字典或列表
    rs_dict = json.loads(rs.text)
    # 取出error
    error_code = rs_dict['error']
    # 如果取出的error为0，表示数据正常，否则没有查询到结果
    if error_code == 0:
        # 从字典中取出数据
        # 根据索引取出天气信息字典
        info_dict = rs_dict['results'][0]
        # 根据字典的key，取出城市名称
        city_name = info_dict['currentCity']
        # 取出pm值
        pm25 = info_dict['pm25']
        # 取出天气信息列表
        weather_data = info_dict['weather_data'][0:2]
        # for循环取出每一天天气的小字典
        content = '当前%s\n' % city_name
        for weather_dict in weather_data:
            # 取出日期，天气，风级，温度
            date = weather_dict['date']
            weather = weather_dict['weather']
            wind = weather_dict['wind']
            temperature = weather_dict['temperature']
            content = content + '日期：%s\t天气：%s\t风级：%s\t温度：%s\tpm值：%s\n' % \
                      (date, weather, wind, temperature, pm25)
        return content


if __name__ == '__main__':
    if not os.path.exists(rec_tmp_dir):
        os.mkdir(rec_tmp_dir)
    itchat.auto_login(hotReload=True, enableCmdQR=2, loginCallback=after_login, exitCallback=after_logout)
    itchat.run()
    itchat.login()
