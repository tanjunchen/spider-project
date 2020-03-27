#!/usr/bin/python
# -*- coding: UTF-8 -*-

import requests


class HtmlDownLoader(object):
    '''
    HTML下载器
    '''

    def download(self, url):
        if url is None:
            return None
        User_Agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
        headers = {
            'User-Agent': User_Agent
        }
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            r.encoding = 'utf-8'
            return r.text
        return None
