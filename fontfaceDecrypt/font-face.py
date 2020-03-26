#!/usr/bin/env python
# -*- coding: utf-8 -*-

from fontTools.ttLib import TTFont
import requests
from datetime import datetime
import json

phone_headers = {
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1'}

web_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
}


def job():
    # 这个是与上面的字体文件相对应的
    number_dict = {
        "period": ".",
        "zero": "0",
        "one": "1",
        "two": "2",
        "three": "3",
        "four": "4",
        "five": "5",
        "six": "6",
        "seven": "7",
        "eight": "8",
        "nine": "9"
    }
    font_url = get_font_ttf()
    dd = font_url.split('/')[-2:]
    name = dd[0] + dd[1]
    font_content = requests.get(font_url, headers=web_headers).content
    # print(font_content)
    with open(name, 'wb') as f:
        f.write(font_content)
    font = TTFont(name)
    font.saveXML(name.replace("ttf", 'xml'))


def get_font_ttf():
    post_data = {"arrCity": "上海",
                 "depCity": "北京",
                 "flightType": "oneWay",
                 "from": "touch_index_guess",
                 "goDate": datetime.now().strftime('%Y-%m-%d'),
                 "sort": "1",
                 "firstRequest": "true",
                 "startNum": 0,
                 "r": 1544747204962,
                 "_v": 2,
                 "underageOption": "",
                 "__m__": "09163ba3379128886841f72d76aa525e"}

    post_data2 = {
        'arrCity': "上海",
        'baby': "0",
        'cabinType': "0",
        'child': "0",
        'depCity': "北京",
        'firstRequest': 'true',
        'from': "touch_index_search",
        'goDate': datetime.now().strftime('%Y-%m-%d'),
        'r': 1544750638857,
        'sort': 5,
        'startNum': 0,
        'underageOption': "",
        '__m__': "fa4863f52526dbbe3b3cba0e3de7e006",
        '_v': 2
    }

    data = requests.post('https://m.flight.qunar.com/touch/api/domestic/flightlist', data=post_data2)
    dd = json.loads(data.text)
    font_src = "https:" + dd['data']['obfuscate']['fontSrc']
    print(font_src)
    return font_src


if __name__ == '__main__':
    job()
