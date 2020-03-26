#!/usr/bin/env python
# -*- coding: utf-8 -*-

import execjs
import base64
import json
import requests

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://www.qimingpian.com',
    'Referer': 'https://www.qimingpian.com/finosda/project/pinvestment',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36',
}

data = {
    'time_interval': '',
    'tag': '',
    'tag_type': '',
    'province': '',
    'lunci': '',
    'page': 1,
    'num': 20,
    'unionid': '',
}


def decrypt(encrypt_data):
    ctx = execjs.compile(open('data.js').read())
    return base64.b64decode(ctx.call('my_decrypt', encrypt_data))


def get_str():
    url = "https://vipapi.qimingpian.com/DataList/productListVip"
    content = requests.post(url, data=data)
    content.encoding = "utf-8"
    if content.ok and content.json() is not None:
        return content.json().get("encrypt_data")
    return None


def get_data():
    str_data = get_str()
    decrypt_data = decrypt(str_data)
    json_data = json.loads(decrypt_data)
    return json_data


if __name__ == '__main__':
    print(get_data())
